## Why

Cancellation currently uses a distributed `cancel-requested`/`cancel-ready` rendezvous that can fail on already-terminal descendants and requires recursive driving, timeout handling, and repeated lifecycle coordination. Cancellation can instead be a retryable two-phase operation in which planning selects and freezes the complete cancellation set before adapter cleanup begins.

## What Changes

- Replace `cancel-requested` and `cancel-ready` with one nonterminal `cancel-pending` lifecycle that identifies executions selected for cancellation and forbids further index mutation.
- Make Phase 1 traverse spawned executions, skip terminal or still-referenced executions, CAS active unreferenced executions to `cancel-pending`, remove their outgoing caller edges and matching cache pointers, and record the complete set to cancel without invoking adapters.
- Treat CAS conflicts as retryable planning races: reread lifecycle and caller references, retry active candidates, and skip candidates that become terminal.
- Make Phase 2 invoke the cancel adapter for each execution selected by Phase 1 and CAS `cancel-pending` to `canceled`, without a readiness state or timeout protocol.
- Make interrupted cancellation resumable from persisted `cancel-pending` records, with idempotent edge removal, cache-pointer deletion, adapter cancellation, and terminal lifecycle transitions.
- Serialize caller registration with cancellation selection so a new valid caller cannot race between the caller-reference check and the `cancel-pending` CAS.
- **BREAKING** Remove the `cancel-requested` and `cancel-ready` execution lifecycle values and expose `cancel-pending` in their place in raw execution records and graph inspection.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `execution-state`: Replace distributed readiness driving with retryable two-phase cancellation and update mutation guards for `cancel-pending`.
- `execution-admin-controls`: Define full and drive cancellation around complete Phase 1 selection and direct Phase 2 adapter cleanup.
- `runtime-execution-records`: Change the lifecycle schema and cancellation transitions, including CAS retry and recovery behavior.
- `execution-call-edges`: Define caller-reference validity and serialization between caller registration and cancellation selection.
- `executor-cancellation`: Remove readiness-timeout coupling while preserving synchronous, idempotent cancel handling.

## Impact

- Affects `src/daggerml/_core/exec_state.py`, runtime cancellation entry points, supervisor drive behavior, lifecycle rendering, and cancellation contract tests.
- Changes persisted execution lifecycle values; existing records containing `cancel-requested` or `cancel-ready` are not accepted by the new lifecycle schema.
- Removes the 60-second cancel-readiness timeout and recursive `cancel-ready` handoff.
- Preserves the adapter cancel request/response envelope and executor-owned resource cleanup behavior.
- Requires updates to runtime, execution-cache, adapter, executor, and architecture documentation that describe cancellation lifecycles.
