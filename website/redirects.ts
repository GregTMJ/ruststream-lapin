// Pages of the earlier site, by their address within a version. Every language of a build gets
// the same set.

// The guides of the earlier site moved to the crate's rustdoc, where each topic is a section of
// the crate overview.
const guides: Record<string, string> = {
  queues: 'index.html#subscribing',
  publishing: 'index.html#publishing',
  'request-reply': 'index.html#request-and-reply',
  documenting: 'index.html#the-asyncapi-document',
  testing: 'index.html#testing',
};

// Pages that moved within the site.
const pages: Record<string, string> = {
  // The benchmarks explain what the architecture costs.
  benchmarks: '/architecture/benchmarks/',
};

/**
 * Old address to its target, relative to the build's base URL. `docsRs` is the crate's rustdoc
 * for the version built, with a trailing slash.
 */
export function redirects(docsRs: string): Record<string, string> {
  return {
    ...Object.fromEntries(
      Object.entries(guides).map(([from, to]) => [from, `${docsRs}ruststream_lapin/${to}`]),
    ),
    ...pages,
  };
}
