## Why

Cancellation currently coordinates work by locking the candidate execution id, which conflicts with the runtime's broader model that uses `cache_key` as the computation identity for execution coordination. This mismatch makes cancellation race against normal launch and resume paths and obscures how an index-rooted cancellation request should transition remote execution state.

## What Changes

- Change cancellation coordination to resolve each candidate execution's `cache_key` from its execution record and acquire the cache-key lock before mutating execution state or invoking cancel updates.
- Change cancellation planning to traverse the full execution graph rooted at the index, derive rooted candidates from that graph, and then drive cancellation through a retryable short-lock loop owned by `Dml.runtime.cancel`.
- Update the cancellation workflow so the synthetic index-root execution is marked `cancel-requested` first, and only transitions to `cancelled` after the full rooted graph has been cancelled successfully.
- Clarify that rooted cancellation starts from the index's synthetic execution record and expands through its recorded execution-id dependencies while using cache-key locks for real execution candidates.
- Clarify that execution `cancelled` status is per-execution only: it means cleanup for that execution is complete and its adapter no longer needs to be called, but graph traversal must still continue until the index-rooted graph is fully cancelled.
- Have `Dml.runtime.cancel` log cancellation diagnostics and return loop statistics such as iteration counts.
- Explicitly document the current sharp edge that if a descendant execution can only be cancelled by adapters unreachable from the index runtime, `Dml.runtime.cancel` may loop indefinitely until the user interrupts it, with `cancel-requested` serving as the only current propagation signal.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `execution-admin-controls`: Change the manual cancellation algorithm to mark the index root `cancel-requested`, traverse the full rooted graph, evaluate global caller ownership from S3-backed reverse edges, and run a retryable cache-key lock loop inside `Dml.runtime.cancel` while cancelling rooted executions.
- `runtime-execution-records`: Clarify that cancellation also acquires execution coordination locks by `cache_key`, even when the operation starts from an execution id or synthetic index id, and that `cancelled` is a per-execution cleanup-complete status rather than a graph-complete status.

## Impact

- Affected code: `src/daggerml/_internal/ops/index.py`, `src/daggerml/_internal/exec_state.py`, and cancellation-focused contract tests.
- Affected behavior: cancellation graph traversal, cancellation state transitions, cancellation lock acquisition, and interaction between cancellation and concurrent execution/resume paths.
- Affected operational behavior: cancellation may remain user-interrupt-driven for remote-only bespoke adapter chains that do not yet honor `cancel-requested` autonomously.
- No new public API surface is expected, but `Dml.runtime.cancel` behavior, returned cancellation statistics, runtime coordination semantics, and cancellation tests will change.
