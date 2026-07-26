## Why

Concurrent execution registration and cancellation rely on compare-and-swap updates to the caller execution record. A registration update that exhausts its retry budget currently logs and returns normally, allowing an adapter invocation to proceed without a durable caller child entry. Cancellation cannot discover or converge such an execution.

Completion and cancellation bookkeeping also silently abandon CAS updates, leaving stale spawned entries and preventing reliable terminal convergence.

## What Changes

- Make caller child-registration CAS exhaustion an explicit failure that prevents adapter invocation and cleans up launch artifacts owned by the failed attempt.
- Make terminal-child bookkeeping retry CAS conflicts with bounded backoff and surface exhaustion rather than silently abandoning the record update.
- Preserve canceled direct children in `spawned_execution_ids`; remove the separate cancellation-only spawned-edge finalization mutation.
- Define execution-record child-list semantics around uncompleted versus terminally completed children.
- Add contention contracts and implementation comments documenting the CAS coordination boundary.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `runtime-execution-records`: Define reliable child registration, terminal child bookkeeping, and canceled-child list semantics.
- `execution-state`: Require bounded retry/backoff and safe failure handling for execution-record coordination updates.
- `execution-call-edges`: Define rollback of an unrealized caller/callee edge when child registration fails.

## Impact

- Affected code: `src/daggerml/_core/exec_state.py` and its runtime coordination tests.
- Affected behavior: adapter-backed execution launch, cancellation traversal, execution graph inspection, and terminal child lineage.
- No public API or dependency changes.
