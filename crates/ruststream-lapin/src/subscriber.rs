//! The subscriber: a stream of AMQP deliveries from one queue consumer, batched on the client for
//! the handlers that take a batch.

use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "testing")]
use futures::future::Either;
use futures::{Stream, StreamExt};
use lapin::options::{BasicConsumeOptions, BasicQosOptions};
use lapin::types::{FieldTable, ShortString};
use lapin::{Channel, Consumer};
use ruststream::{BatchSubscriber, BufferedSubscriber, Subscriber};
use tracing::{info, warn};

use crate::broker::AmqpConnection;
use crate::convert;
use crate::delay::DelayContext;
use crate::error::AmqpError;
#[cfg(feature = "testing")]
use crate::in_process::BusDeliveries;
use crate::message::LapinMessage;
use crate::queue::QueueSpec;
use crate::topology;

/// The first wait between two attempts to open a subscription again, doubled after every failed
/// attempt up to [`REOPEN_MAX_WAIT`].
const REOPEN_FIRST_WAIT: Duration = Duration::from_millis(100);
/// The longest wait between two attempts: a server that is down for long is tried this often.
const REOPEN_MAX_WAIT: Duration = Duration::from_secs(10);

/// A consumer on one queue, yielding [`LapinMessage`] deliveries.
///
/// Created by subscribing a [`RabbitQueue`](crate::RabbitQueue) descriptor (or a bare queue name)
/// through [`LapinBroker`](crate::LapinBroker). The subscriber owns a dedicated channel;
/// dropping it closes that channel and the broker redelivers whatever was unacknowledged.
///
/// A subscription outlives its connection: when the server restarts, fails over or becomes
/// unreachable, the subscriber dials the server again and opens the same consumer there, with the
/// same declaration and prefetch, retrying until the server answers or the broker shuts down.
/// Deliveries in flight when the connection went away cannot be settled any more, and the server
/// delivers them again.
///
/// Back-pressure: the broker stops pushing once
/// [`prefetch`](crate::LapinBroker::prefetch) unacknowledged deliveries are in flight, so
/// consuming slower slows the producer side down instead of buffering without bound.
///
/// It is a [`BatchSubscriber`] as well as a [`Subscriber`]: AMQP has no wire batch, so a batch
/// handler's size is honoured by collecting the deliveries here (see the
/// [impl](#impl-BatchSubscriber-for-LapinSubscriber)).
pub struct LapinSubscriber {
    deliveries: BufferedSubscriber<Deliveries>,
    queue: String,
}

impl LapinSubscriber {
    pub(crate) fn new(
        connection: Arc<AmqpConnection>,
        subscription: Subscription,
        opened: OpenedConsumer,
    ) -> Self {
        let queue = subscription.spec.name.clone();
        let batch_wait = subscription.spec.batch_wait;
        let deliveries = Deliveries::Amqp(AmqpDeliveries {
            connection,
            subscription,
            opened,
        });
        Self {
            deliveries: BufferedSubscriber::new(deliveries).max_wait(batch_wait),
            queue,
        }
    }

    /// A subscriber on the in-process transport, batching on the client with the descriptor's
    /// own deadline as the live subscriber does.
    #[cfg(feature = "testing")]
    pub(crate) fn in_process(
        deliveries: BusDeliveries,
        queue: String,
        batch_wait: Duration,
    ) -> Self {
        Self {
            deliveries: BufferedSubscriber::new(Deliveries::InProcess(deliveries))
                .max_wait(batch_wait),
            queue,
        }
    }

    /// The queue this subscriber consumes from.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }
}

impl std::fmt::Debug for LapinSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LapinSubscriber")
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}

impl Subscriber for LapinSubscriber {
    type Message = LapinMessage;
    type Error = AmqpError;

    /// Streams deliveries as they arrive; the stream ends when the server cancels the consumer or
    /// the broker shuts down. A lost connection or channel does not end it: the subscriber opens
    /// the consumer again and the stream goes on from there.
    ///
    /// # Cancel safety
    ///
    /// Polling is cancel safe (no delivery is lost by dropping the stream between polls), and
    /// the stream can be re-created by calling `stream` again: deliveries buffer in the
    /// consumer, not in the returned stream.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.deliveries.stream()
    }
}

/// Batches are assembled on the client: AMQP delivers one `basic.deliver` at a time, so there is
/// no wire batch to ask the broker for, and a batch closes on the registration's size or on the
/// descriptor's [`batch_wait`](crate::RabbitQueue::batch_wait), whichever comes first.
///
/// A batch can only hold what the broker has already pushed, so a subscription whose
/// [`prefetch`](crate::RabbitQueue::prefetch) window is narrower than the registration's batch size
/// yields batches capped by the window rather than by the size.
impl BatchSubscriber for LapinSubscriber {
    type Batch = Vec<LapinMessage>;

    /// # Cancel safety
    ///
    /// As cancel safe as [`stream`](Subscriber::stream) between polls; dropping the returned
    /// stream abandons the batch being assembled, and the broker redelivers those deliveries when
    /// the channel closes.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        self.deliveries.batches(size)
    }
}

/// One delivery at a time, from whichever transport the broker was connected to: everything above
/// it batches on the client.
///
/// Without the `testing` feature the wire consumer is the only variant, so the type is the
/// consumer and the `match` in [`stream`](Subscriber::stream) resolves at compile time.
// The live consumer is the larger variant on purpose: it is the one a production build has, and
// boxing it would put an allocation on the service's own path to shrink a test build.
#[cfg_attr(feature = "testing", allow(clippy::large_enum_variant))]
enum Deliveries {
    Amqp(AmqpDeliveries),
    #[cfg(feature = "testing")]
    InProcess(BusDeliveries),
}

#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Deliveries>() == size_of::<AmqpDeliveries>());

impl Subscriber for Deliveries {
    type Message = LapinMessage;
    type Error = AmqpError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        match self {
            #[cfg(not(feature = "testing"))]
            Self::Amqp(consumer) => consumer.stream(),
            #[cfg(feature = "testing")]
            Self::Amqp(consumer) => Either::Left(consumer.stream()),
            #[cfg(feature = "testing")]
            Self::InProcess(deliveries) => Either::Right(deliveries.stream()),
        }
    }
}

/// What a subscription is opened from, kept so it can be opened again on a new connection.
pub(crate) struct Subscription {
    pub(crate) spec: QueueSpec,
    /// The arguments the queue is declared with, present when the broker declares topology.
    pub(crate) declare: Option<FieldTable>,
    pub(crate) prefetch: Option<NonZeroU16>,
}

/// A consumer opened on a channel of its own.
pub(crate) struct OpenedConsumer {
    // Kept alive for the lifetime of the consumer: dropping the channel cancels the consumer
    // server-side.
    channel: Channel,
    consumer: Consumer,
    // Present only when the descriptor opted into a native delay queue; threaded into every
    // delivery so `nack_after` can re-publish to the waiting queue.
    delay: Option<DelayContext>,
}

impl Subscription {
    /// Opens a channel, declares the topology when the broker opted in, sets the prefetch, and
    /// starts the consumer.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Closed`] after shutdown, [`AmqpError::Connect`] when the server cannot
    /// be reached, [`AmqpError::Declare`] when the declaration fails, and
    /// [`AmqpError::Subscribe`] when the channel or the consumer cannot be opened.
    pub(crate) async fn open(&self, conn: &AmqpConnection) -> Result<OpenedConsumer, AmqpError> {
        let spec = &self.spec;
        let channel = conn
            .live_connection(&spec.name)
            .await?
            .create_channel()
            .await
            .map_err(AmqpError::subscribe)?;

        if let Some(arguments) = &self.declare {
            topology::declare(&channel, spec, arguments.clone()).await?;
        }
        if let Some(prefetch) = self.prefetch {
            channel
                .basic_qos(prefetch.get(), BasicQosOptions::default())
                .await
                .map_err(AmqpError::subscribe)?;
        }

        // A native delay backend re-publishes the delayed copy on the same channel the delivery
        // is acked on, so no extra channel is created and the publish orders naturally before the
        // ack (duplicate-not-loss).
        let delay = spec
            .delay
            .as_ref()
            .map(|delay| DelayContext::new(channel.clone(), delay.target_for(&spec.name)));

        let consumer = channel
            .basic_consume(
                convert::short(&spec.name, "queue name")?,
                ShortString::default(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(AmqpError::subscribe)?;

        Ok(OpenedConsumer {
            channel,
            consumer,
            delay,
        })
    }
}

/// The wire consumer, one `basic.deliver` at a time.
struct AmqpDeliveries {
    // Threaded into every delivery, so a settlement after shutdown is refused instead of waiting
    // on an answer lapin never gives.
    connection: Arc<AmqpConnection>,
    subscription: Subscription,
    opened: OpenedConsumer,
}

impl AmqpDeliveries {
    fn stream(&mut self) -> impl Stream<Item = Result<LapinMessage, AmqpError>> + Send + '_ {
        futures::stream::unfold(self, move |this| async move {
            loop {
                match this.opened.consumer.next().await {
                    Some(Ok(delivery)) => {
                        let message = LapinMessage::from_delivery(
                            delivery,
                            Some(Arc::clone(&this.connection)),
                            this.opened.delay.clone(),
                        );
                        return Some((Ok(message), this));
                    }
                    Some(Err(err)) => return Some((Err(AmqpError::consume(err)), this)),
                    None => {
                        if !this.reopen().await {
                            return None;
                        }
                    }
                }
            }
        })
    }

    /// Opens the subscription again once the consumer ended with its channel, and answers whether
    /// it did.
    ///
    /// The channel goes when the connection does, and when the server closes it on a channel
    /// error. A consumer that ended on a live channel was cancelled by the server (its queue was
    /// deleted) and ends the stream, as does the broker's shutdown.
    ///
    /// # Cancel safety
    ///
    /// Cancel safe: the new consumer replaces the old one only once it is open, so a reopen
    /// dropped halfway starts over on the next poll.
    async fn reopen(&mut self) -> bool {
        if self.connection.is_closed() || self.opened.channel.status().connected() {
            return false;
        }
        let queue = &self.subscription.spec.name;
        let mut wait = REOPEN_FIRST_WAIT;
        loop {
            match self.subscription.open(&self.connection).await {
                Ok(opened) => {
                    info!(queue = %queue, "subscription reopened after its channel was lost");
                    self.opened = opened;
                    return true;
                }
                Err(AmqpError::Closed { .. }) => return false,
                Err(err) => {
                    warn!(
                        queue = %queue,
                        error = %err,
                        retry_in = ?wait,
                        "cannot reopen the subscription after its channel was lost",
                    );
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(REOPEN_MAX_WAIT);
                }
            }
        }
    }
}
