# Extension Model

DaggerML authoring can initially describe work without selecting its final
runtime object. `daggerml.contrib.api.funkify()` produces a delayed runnable.
When a DAG stages it, `DelayedActionCodec` asks the selected adapter to lower it
to a concrete `Runnable`.

The resulting path is:

1. A delayed runnable selects an adapter and executor URI.
2. The adapter's `resolve_runnable(uri, kwargs, sub)` delegates to the matching
   executor and produces a `Runnable` with an adapter executable.
3. The runtime invokes that executable with an operation-specific JSON payload.
4. The adapter performs one bounded operation, usually by delegating to an
   executor.
5. The runtime, not the extension, owns cache publication, durable resume
   state, execution records, and cancellation coordination.

An adapter is the transport boundary. An executor implements backend behavior.
An executor can wrap a nested runnable, as Docker, SSH, and Batch do, or run a
leaf operation, as `script` does.

See [Adapters and executors](adapters-and-executors.md) for the lifecycle and
[Codec contracts](../reference/codec-contracts.md) for staging values.
