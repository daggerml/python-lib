## Why

Execution lifecycle writers currently rely on inconsistent local checks, allowing stale CAS writers to overwrite cancellation or terminal state and allowing cancellation to miss incompletely registered children. The execution state contract must make lifecycle transitions, lock authority, and lineage bookkeeping explicit so cancellation remains synchronous and race-safe.

## What Changes

- Define an absorbing lifecycle transition contract: only `pending -> running`, `pending|running -> cancel-pending`, `running -> succeeded|failed`, and `cancel-pending -> canceled` are permitted.
- Require every execution `state.json` mutation to use CAS and require the execution driver lock for every changed field except result publication and the `spawned_execution_ids` and `child_execution_ids` lineage summaries.
- Require lock-free result and lineage writers to reevaluate lifecycle on every CAS attempt; result publication and child spawning require `running`, while terminal-child bookkeeping may complete for `running` or `cancel-pending` callers.
- Complete terminal-child `spawned_execution_ids -> child_execution_ids` bookkeeping before surfacing cancellation when the caller is already `cancel-pending`.
- Make child registration publish the caller's spawned summary before adapter invocation, reject a caller that is no longer `running`, and remove only the attempted edge, matching cache pointer, and unchanged owned execution objects on a rejected fresh launch.
- Make cancellation Phase 1 transition only `pending` or `running` executions to `cancel-pending`, reconstruct existing `cancel-pending` work without rewriting it, skip terminal executions, and reevaluate lifecycle and caller edges after CAS contention.
- Preserve cancellation ownership of `cancel-pending` records by rejecting unrelated control updates such as invalidation.
- Make cancellation Phase 2 invoke adapters only for `cancel-pending`, accept `canceled` as concurrent completion, and warn then drop any other unexpected lifecycle from the drive set.
- Add deterministic race and lifecycle-matrix coverage for activation, result publication, child registration/completion, and both cancellation phases.
- Preserve the existing execution metadata, state, driver, cache-pointer, and edge schemas.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `runtime-execution-records`: Define field-level lock authority, permitted lifecycle transitions, terminal absorption, and guarded result and lineage mutations without changing record shape.
- `execution-call-edges`: Tighten child registration and terminal-child bookkeeping around caller cancellation and owned-artifact cleanup.
- `execution-state`: Require lifecycle-safe CAS mutation, exact Phase 1 transition sources, and warning-and-drop handling for unexpected Phase 2 lifecycle values.
- `execution-admin-controls`: Clarify resumable cancellation behavior when Phase 2 observes lifecycle drift outside `cancel-pending`.
- `unified-dml-surface`: Align public runtime cancellation completion and exhaustion semantics with explicit Phase 2 lifecycle filtering.

## Impact

- Primary implementation: `src/daggerml/_core/exec_state.py` and activation/commit call sites in `src/daggerml/_core/index.py`.
- Primary verification: execution-coordination contract tests and deterministic runtime cancellation integration tests.
- Documentation: execution/runtime-state architecture and the flaky-CI investigation conclusions.
- Public method signatures, adapter request/response shapes, persisted JSON fields, object paths, and dependencies remain unchanged.
