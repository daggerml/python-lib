# Remote Integrations

Remote execution coordination is owned by `ExecutionState` under the configured
remote root. It keeps an advisory lock per cache key, an active-execution
pointer, immutable launch state, mutable execution records, cancellation
targets, and adapter scratch space.

An adapter request contains a remote root and scratch URI. Detached executors
should use the supplied scratch URI rather than inventing process-local handoff
state. The built-in Docker and Batch executors write nested adapter input and
output below that URI so a later poll can observe the same result.

An integration normally needs S3-compatible access to the configured remote
root. `S3Store` is useful for integration-owned artifacts: its default prefix
is derived from `remote.root` under `data/`, separate from DaggerML's protocol
objects. It is not a replacement for runtime coordination records.

Remote integrations should tolerate retried polls and cancellation requests.
Do not assume the process that called `start()` will perform `poll()` or
`cancel()`.
