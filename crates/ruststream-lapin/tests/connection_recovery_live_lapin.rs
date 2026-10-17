//! What a running service does when its connection to the server goes away.
//!
//! A server restart, a node failover and a network cut all end the same way for a client: the
//! connection is gone and every channel on it with it. The service must come back on its own once
//! the server answers again, with its subscriptions consuming as before, instead of staying up
//! with nothing consuming.
//!
//! The service reaches the server through a proxy the test owns, so a test can cut the link the
//! way a network does and keep the server unreachable for a while.
//!
//! Every test is a no-op unless `AMQP_TEST_URL` points at a broker (see `just test-brokers`).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt};
use lapin::options::QueueDeleteOptions;
use lapin::{Connection, ConnectionProperties};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_lapin::prelude::*;
use ruststream_lapin::{AmqpError, LapinMessage};
use serde::{Deserialize, Serialize};
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, watch};
use tokio::task::{AbortHandle, JoinHandle};

mod live;

const WAIT: Duration = Duration::from_secs(15);
/// How long the server stays unreachable in the outage case: long enough for the service to fail
/// several attempts to reach it.
const OUTAGE: Duration = Duration::from_secs(2);

/// Fixed names, because the attribute takes a constant; each test removes them before and after.
const ORDERS: &str = "ruststream-connection.orders";
const RECEIPTS: &str = "ruststream-connection.receipts";

/// The broker address, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing address
/// fails the suite instead of skipping it.
fn amqp_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

#[derive(Deserialize, Serialize)]
struct Order {
    id: u64,
}

#[derive(Deserialize, Serialize, Outgoing)]
struct Receipt {
    id: u64,
}

#[subscriber(RabbitQueue::new(ORDERS), reply(RECEIPTS))]
async fn take_order(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

/// What the proxy does with a connection the service opens.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Link {
    /// Carries it to the server.
    Up,
    /// Drops it at once, as an unreachable server answers.
    Down,
    /// Holds it unanswered until the proxy is up again, so the dial stays in flight.
    Held,
}

/// A TCP proxy between the service and the server, which the test cuts like a network does.
struct Proxy {
    addr: SocketAddr,
    links: Arc<Mutex<Vec<AbortHandle>>>,
    mode: watch::Sender<Link>,
    /// Signalled when a connection is held.
    held: Arc<Notify>,
    accept: JoinHandle<()>,
}

impl Proxy {
    async fn start(server: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("proxy bind");
        let addr = listener.local_addr().expect("proxy address");
        let links = Arc::new(Mutex::new(Vec::new()));
        let (mode, _) = watch::channel(Link::Up);
        let held = Arc::new(Notify::new());
        let accept = tokio::spawn({
            let links = Arc::clone(&links);
            let current = mode.subscribe();
            let held = Arc::clone(&held);
            async move {
                loop {
                    let Ok((mut inbound, _)) = listener.accept().await else {
                        return;
                    };
                    let server = server.clone();
                    let mut current = current.clone();
                    let held = Arc::clone(&held);
                    let link = tokio::spawn(async move {
                        if *current.borrow_and_update() == Link::Held {
                            held.notify_one();
                            let _ = current.wait_for(|mode| *mode != Link::Held).await;
                        }
                        if *current.borrow() == Link::Down {
                            return;
                        }
                        let Ok(mut outbound) = TcpStream::connect(&server).await else {
                            return;
                        };
                        let _ = copy_bidirectional(&mut inbound, &mut outbound).await;
                    });
                    links.lock().expect("links").push(link.abort_handle());
                }
            }
        });
        Self {
            addr,
            links,
            mode,
            held,
            accept,
        }
    }

    /// Drops every link the proxy carries, which the client sees as a connection reset.
    fn cut(&self) {
        for link in self.links.lock().expect("links").drain(..) {
            link.abort();
        }
    }

    fn set(&self, mode: Link) {
        self.mode.send_replace(mode);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
        self.cut();
    }
}

/// `url` with its host and port replaced by `addr`, keeping the credentials and the vhost.
fn through(url: &str, addr: SocketAddr) -> String {
    let (scheme, rest) = url.split_once("://").expect("an amqp url");
    let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    let credentials = authority
        .rsplit_once('@')
        .map_or(String::new(), |(credentials, _)| format!("{credentials}@"));
    format!("{scheme}://{credentials}{addr}{path}")
}

/// The `host:port` of `url`.
fn server(url: &str) -> String {
    let rest = url.split_once("://").expect("an amqp url").1;
    let authority = &rest[..rest.find('/').unwrap_or(rest.len())];
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    if host.contains(':') {
        host.to_owned()
    } else {
        format!("{host}:5672")
    }
}

async fn next<S>(stream: &mut S) -> LapinMessage
where
    S: Stream<Item = Result<LapinMessage, AmqpError>> + Unpin,
{
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok")
}

async fn delete_queues(url: &str) {
    let cleanup = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("cleanup connect");
    let channel = cleanup.create_channel().await.expect("cleanup channel");
    for queue in [ORDERS, RECEIPTS] {
        let _ = channel
            .queue_delete(queue.into(), QueueDeleteOptions::default())
            .await;
    }
    cleanup
        .close(200, "OK".into())
        .await
        .expect("cleanup close");
}

/// Runs the service through a proxy, cuts the proxy while the service runs, keeps the server
/// unreachable for `outage`, and checks that an order published after the cut is answered.
async fn answers_after_the_connection_is_lost(outage: Duration) {
    let Some(url) = amqp_url() else { return };
    delete_queues(&url).await;
    let proxy = Proxy::start(server(&url)).await;

    // The client speaks to the server directly: it declares both queues before the service is up,
    // publishes the orders and reads the receipts.
    let client = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("client connect");
    let orders = client
        .subscribe(RabbitQueue::new(ORDERS))
        .await
        .expect("declare the orders queue");
    drop(orders);
    let mut receipts = client
        .subscribe(RabbitQueue::new(RECEIPTS))
        .await
        .expect("subscribe the receipts");
    let mut receipts = Box::pin(receipts.stream());
    let publisher = client.publisher(Publish::default());
    let order = async |id: u64| {
        publisher
            .publish(
                OutgoingMessage::new(ORDERS, &serde_json::to_vec(&Order { id }).expect("encode")),
                None,
            )
            .await
            .expect("publish an order");
    };
    let receipt = async |receipts: &mut _| {
        let message = next(receipts).await;
        let receipt: Receipt = serde_json::from_slice(message.payload()).expect("decode");
        message.ack().await.expect("ack the receipt");
        receipt.id
    };

    let stop = Arc::new(Notify::new());
    let waiter = Arc::clone(&stop);
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new(through(&url, proxy.addr))
            .declare_topology(true)
            .prefetch(nonzero!(8)),
        |b| {
            b.include(take_order).out_reply(Publish::default());
        },
    );
    let running = tokio::spawn(app.run_until(async move { waiter.notified().await }));

    order(1).await;
    assert_eq!(
        receipt(&mut receipts).await,
        1,
        "the service answers before the cut"
    );

    proxy.set(Link::Down);
    proxy.cut();
    order(2).await;
    tokio::time::sleep(outage).await;
    proxy.set(Link::Up);
    assert_eq!(
        receipt(&mut receipts).await,
        2,
        "the service must reach the server again and answer an order published after the cut",
    );

    stop.notify_one();
    tokio::time::timeout(WAIT, running)
        .await
        .expect("the service shuts down")
        .expect("app task did not panic")
        .expect("run_until succeeded");
    drop(receipts);
    client.shutdown().await.expect("client shutdown");
    delete_queues(&url).await;
}

// A link cut once, with the server answering at once: the service comes back on the first try.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_service_answers_again_after_its_connection_is_cut() {
    answers_after_the_connection_is_lost(Duration::ZERO).await;
}

// A server that stays unreachable for a while: the service keeps trying until it answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_service_answers_again_after_the_server_was_unreachable() {
    answers_after_the_connection_is_lost(OUTAGE).await;
}

// A subscription that dials the server again while the broker shuts down ends instead of
// installing a connection nobody will close, and a publisher paired before the shutdown reports
// the broker closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dial_in_flight_at_shutdown_ends_the_subscription() {
    let Some(url) = amqp_url() else { return };
    delete_queues(&url).await;
    let proxy = Proxy::start(server(&url)).await;

    let broker = LapinBroker::new(through(&url, proxy.addr))
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");
    let mut orders = broker
        .subscribe(RabbitQueue::new(ORDERS))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(Publish::default());

    proxy.set(Link::Held);
    proxy.cut();
    let consuming = tokio::spawn(async move {
        let mut stream = Box::pin(orders.stream());
        let lost = stream.next().await;
        assert!(
            matches!(lost, Some(Err(_))),
            "the stream reports the lost connection"
        );
        // The subscription now dials the server again, and the proxy holds that dial.
        stream.next().await.map(|item| item.is_ok())
    });
    tokio::time::timeout(WAIT, proxy.held.notified())
        .await
        .expect("the subscription dials the server again");

    broker.shutdown().await.expect("shutdown");
    proxy.set(Link::Up);
    let after = tokio::time::timeout(WAIT, consuming)
        .await
        .expect("the subscription answers")
        .expect("consuming task did not panic");
    assert_eq!(
        after, None,
        "a subscription whose dial outlived the shutdown must end"
    );

    let refused = publisher
        .publish(OutgoingMessage::new(ORDERS, b"{}"), None)
        .await;
    assert!(
        matches!(refused, Err(AmqpError::Closed { .. })),
        "a publisher paired before the shutdown must report the broker closed, got {refused:?}",
    );
    delete_queues(&url).await;
}
