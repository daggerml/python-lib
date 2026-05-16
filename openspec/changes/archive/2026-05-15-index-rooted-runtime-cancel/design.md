## Context

Current cancellation is planned from execution ids and stops after writing `cancel-requested` into execution state. That leaves the caller-facing API mismatched with user intent and leaves contrib-managed external work running until some later poll notices the control bit. The new flow starts from an index, treats that `index_id` as a synthetic execution root in the same `dml/exec/state/*` and `dml/exec/edges/*` namespace as normal executions, freezes that index by moving it to `indexes/.cancelled/<id>.json` under lock, computes the rooted active execution closure from that frozen index root, and invokes executor-owned cancel behavior as a bounded sweep.

The design must preserve two existing constraints: execution state remains the durable source of truth for execution status, and contrib adapters remain the transport layer that simply forwards `execution_status` and `cancel_requested_by` to executors. Executors, not adapters, own cleanup of external resources.

## Goals / Non-Goals

**Goals:**
- Add a caller-facing `Dml.runtime.cancel(index_id)` entrypoint.
- Persist index-root lineage in the same S3 execution state and edge namespaces as normal executions.
- Freeze an index atomically so no new descendants can be attached during cancellation.
- Plan cancellation from the frozen index root instead of from user-supplied execution ids.
- Recheck terminal state, live-caller ownership, and `cancel-requested` writes under a per-execution lock.
- Treat `cancel-requested` as an executor update step and let each executor tear down its own external resources.
- Keep cancellation bounded: once the sweep has run, remove the temporary cancelled-index marker.

**Non-Goals:**
- Clearing or rewriting persisted execution-record `state` after cancellation.
- Introducing a long-lived cancellation orchestration state machine.
- Guaranteeing that already marked `cancel-requested` executions are reaped immediately if no adapter cycle is triggered.
- Changing adapters to own backend-specific cancellation logic.

## Decisions

### Index-rooted cancellation is the only caller-facing cancel API
`Dml.runtime.cancel(index_id)` is the new orchestration boundary. Users cancel work by naming the mutable index they own rather than execution ids discovered from internals. This matches the object users actually manipulate.

Alternative considered: keep an execution-id-based cancel API and add a thin helper that resolves execution ids from an index. Rejected because it preserves the wrong ownership boundary and makes caller semantics depend on runtime internals.

### A cancelled index is represented by an atomic move to `indexes/.cancelled/<id>.json`
The runtime will lock the index, move the live index object to `indexes/.cancelled/<id>.json`, and release the lock before planning the sweep. The moved object is a short-lived freeze marker, not a new persistent state machine. Any code that mutates indexes must treat the live path as absent once the move succeeds.

Alternative considered: keep the index in place and add a sidecar tombstone. Rejected because two objects would need to be consulted to know whether the index is still mutable.

### Indexes are synthetic execution roots in S3 state and edge storage
The runtime will persist an `exec/state/<index_id>.json` object for each live index and will record rooted lineage from that index using the same canonical `exec/edges/<callee>/<caller>.json` namespace used for execution-to-execution dependencies. This keeps rooted traversal, caller counting, and cancellation planning in one graph model rather than splitting lineage between local index pointers and remote execution records.

Alternative considered: keep index lineage in a separate `indexes/`-specific graph namespace. Rejected because cancellation would need custom traversal and special-case caller counting for index roots.

### Cancellation is a bounded sweep, not an eventual workflow
“Done” means the runtime completed one rooted cancellation sweep: it walked the active call graph from the frozen index, identified eligible executions, marked them `cancel-requested` under lock, invoked their adapters in cancel mode, and then removed the temporary cancelled-index marker. Some executions may remain in `cancel-requested` afterward; they are no longer active and can be reaped later.

Alternative considered: keep the cancelled-index marker until every descendant becomes terminal. Rejected because it turns cancellation into a long-running coordinator and complicates rollback and observability.

### Execution eligibility is decided under a per-execution lock
For each candidate id in the rooted set, the runtime acquires that id's lock, rereads current state, skips `succeeded`, `failed`, and `cancelled`, counts active callers while excluding `cancel-requested`, writes `cancel-requested` before any adapter cancel call, and adds that record's dependencies into the work set regardless of whether the adapter is invoked. If a caller resumes after the lock releases, it must create a new execution rather than attaching work to the cancelled one.

Alternative considered: plan from one global snapshot without per-execution rechecks. Rejected because caller ownership races would make shared dependencies unsafe to cancel.

### Executors handle cancellation according to whether update normally dispatches to `runnable.sub`
Executors fall into two groups:
- Update-dispatch executors such as `ssh` continue to call `runnable.sub` when `execution_status == "cancel-requested"`, then perform any executor-owned cleanup.
- Detached-work executors such as `batch`, `docker`, `script`, and `cfn` do not call `runnable.sub` on cancel updates; they cancel their own external resources directly and return quickly.

This keeps adapters transport-only and preserves the runtime rule that executor behavior is backend-specific.

### Cancel invocation success is operational success, not DAG success
`runtime.cancel` owns the S3/index/execution-state side effects. Executor cancel calls return a good operational result when the cancel update was processed without transport/runtime exceptions, even if the underlying job is being rolled back asynchronously. This is especially important for CloudFormation, where rollback must start quickly but finish later.

## Risks / Trade-offs

- [Executions can remain in `cancel-requested` after the sweep] → Treat `cancel-requested` as non-active and add a later reap path if needed.
- [Detached backends differ in what “cancel” means] → Keep the shared contract small and specify backend-specific teardown rules per executor.
- [CloudFormation rollback is asynchronous] → Return quickly with stack context and let backend progress continue outside the bounded sweep.
- [Short-lived cancelled-index marker reduces auditability] → Rely on execution state and normal logs for post-hoc inspection rather than keeping the freeze marker permanently.
