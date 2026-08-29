# Remote Integrations

Remote execution coordination is owned by `ExecutionState` under the configured
remote root. A plain cache pointer names split immutable metadata, shared
semantic state, and caller driver objects. The driver owns locking, durable
adapter state, shared retry timing, and cleanup outcome; state owns lifecycle,
result, lineage, cancelation, and invalidation. Caller edges and adapter scratch
space remain separate.

An adapter request contains a remote root and scratch URI. Detached executors
should use the supplied scratch URI rather than inventing process-local handoff
state. The built-in Docker and Batch executors write nested adapter input and
output below that URI so a later repeated invoke can observe the same result.

An integration normally needs S3-compatible access to the configured remote
root. `S3Store` is useful for integration-owned artifacts: its default prefix
is derived from `remote.root` under `data/`, separate from DaggerML's protocol
objects. It is not a replacement for runtime coordination records.

Remote integrations must preserve continuation across fresh invoke calls and
tolerate retries, shared delay hints, idempotent cleanup, and cancellation.
There is no protocol-level poll request. Ephemeral Docker and Batch wrappers
must finish or terminally record nested cleanup before exit; outer cleanup later
removes wrapper resources. Do not assume the process that called `start()` will
perform `poll()`, `cleanup()`, or `cancel()`.
