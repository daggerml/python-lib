## Why

Funk runtimes currently publish a result and terminal lifecycle together, allowing callers to return cached results without giving adapters a final opportunity to prune external resources. The unified execution record also forces runtime result publication, caller coordination, adapter continuation state, and lineage bookkeeping through one lock and CAS domain despite their different owners.

## What Changes

- **BREAKING** Replace each unified execution record with immutable `metadata.json`, shared semantic `state.json`, and caller-owned `driver.json` objects; no legacy execution-record compatibility is retained while the protocol remains v0.
- Let funk runtimes publish their own `result_ref` directly to `state.json` through guarded CAS without acquiring the driver lock; callers continue to own lifecycle transitions and adapter-error DAG publication.
- Serialize adapter calls and adapter continuation state through the lock in `driver.json`, with bounded robust CAS retries for both driver and state mutations.
- Add a shared `not_before` timestamp so an adapter `retry` response can apply backpressure to every caller for the execution.
- **BREAKING** Add an explicit adapter `cleanup` operation and replace invoke response statuses with `success`, `retry`, and failure codes. Repeated `invoke` remains the only start/poll operation and adapters infer continuation from execution ID and adapter state.
- Track cleanup completion or failure independently from execution lifecycle; a populated runtime result establishes execution success and cleanup return codes never change that lifecycle.
- Update built-in local, Lambda, script, Docker, Batch, and SSH adapter/executor paths to support explicit, idempotent cleanup and shared retry timing.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `execution-state`: Split semantic state from caller locking and define unlocked guarded state CAS behavior.
- `runtime-execution-records`: Replace the unified record schema and revise result publication, lifecycle, cache, inspection, and retry behavior.
- `adapter-operation-protocol`: Add cleanup, simplify response codes, and define shared backpressure semantics.
- `execution-call-edges`: Move forward lineage summaries to `state.json` and replace embedded-lock mutation with guarded CAS.
- `executor-cancellation`: Align executor resource teardown with the new explicit cleanup operation while preserving cancellation semantics.

## Impact

The change affects `daggerml._core.exec_state`, runtime/index orchestration, public runtime and cache inspection payload assembly, adapter request/response types, `AdapterBase`, `ExecutorBase`, all built-in contrib executors, remote execution objects under `exec/`, and their contract and integration tests. Extension adapters and executors must adopt the v0 cleanup and response-code protocol. Human-facing runtime, adapter, executor, and plugin documentation must be updated.
