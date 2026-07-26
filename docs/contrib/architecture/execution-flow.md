# Execution Flow

This is the high-level path from a contrib-authored value to a terminal result.

## 1. Authoring builds delayed values

The process starts in Python code with values such as:

- `DelayedRunnable`
- `DelayedRef`
- `DelayedLoad`
- compiled `dagclass` members

At this point, contrib is still describing work, not executing it.

## 2. DAG staging lowers delayed values

When the DAG stages values, contrib lowering resolves:

- local refs through the current DAG namespace,
- external loads through committed DAG state,
- delayed runnables through the selected adapter's `resolve_runnable(...)` method.

That is where contrib turns declarative wrappers into concrete `Runnable` objects with concrete adapters and targets.

## 3. The adapter boundary receives an operation-specific payload

`AdapterInvokeRequest` is given:

- the `Runnable`
- `cache_key`
- `execution_id`
- `remote`
- optional persisted `state`

`AdapterCancelRequest` is given the execution id, cancellation requester, saved state, and an argv pointer from the execution-owned cancel target. It is not reconstructed from the mutable active ref.

Adapters are expected to perform one bounded step and return canonical JSON-compatible output.

## 4. Executors handle backend-specific behavior

For the built-in local path, `LocalAdapter.send(...)` looks up the executor by `(adapter="local", runnable.target.uri)` and delegates to `spec.handle(...)`.

`ExecutorBase.handle(...)` dispatches the explicit operation. For invocation it decides whether to:

- call `start(...)` for a first launch,
- call `poll(...)` for a resumed launch,
- call `cancel(...)` for a cancel operation.

That shared control flow is why detached backends can still fit the same runtime model as synchronous ones.

## 5. The runtime publishes the terminal result

Executors do not publish final cache entries themselves. They return terminal status to the runtime, and the runtime-owned execution path publishes cache or failure results after observing `succeeded` or `failed`.

This keeps cache publication centralized even when the actual work happened in another process, a container, a remote machine, or a cloud service.

## Consequences for contrib authors

- Wrapper order matters because each wrapper changes the next lowering step.
- The innermost script callable must be self-contained because it is serialized and replayed elsewhere.
- Detached executors must return durable first-launch state because later polls may happen in a different process.

For the lower-level state and supervisor details, continue to [supervisor and state](supervisor-and-state.md).
