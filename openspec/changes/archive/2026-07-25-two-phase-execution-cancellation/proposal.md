## Why

The current lifecycle combines cancellation planning, active ownership, and adapter cleanup in one flow. That creates races between cancellation and relaunch, makes cancellation payloads depend on a mutable cache-key pointer, and does not provide a clear distributed cleanup protocol for nested executions.

The runtime needs to revoke execution ownership first, then clean up external work asynchronously and from leaves toward callers.

## What Changes

- Split cancellation into a lock-protected planning phase and an asynchronous distributed cleanup phase.
- Introduce `cancel-requested` as the lifecycle state recorded during cancellation planning.
- Move, rather than regenerate, each active argv ref to `refs/cancel-targets/<execution_id>.json` during Phase 1.
- Make Phase 1 traverse a user-initialized set of execution IDs, remove active ownership conditionally, and enqueue callees.
- Introduce distinct `AdapterInvokeRequest` / `AdapterInvokeResponse` and `AdapterCancelRequest` / `AdapterCancelResponse` contracts.
- Make Phase 2 wait for callee readiness, invoke cancellation through the callee's cancel target, and support a 60-second readiness timeout.
- Clarify `cancel-ready` as the intermediate state used for distributed cleanup and infrastructure reaping.
- Update execution, adapter, remote-protocol, and lifecycle documentation to describe the new ownership and protocol boundaries.
- Add concurrency, ordering, timeout, ref-move, and adapter-contract coverage.

## Capabilities

### New Capabilities

- `adapter-operation-protocol`: Separate invocation and cancellation request/response contracts for adapter operations.

### Modified Capabilities

- `execution-state`: Change cancellation planning, active-ref ownership removal, cancel-target refs, and Phase 2 readiness handling.
- `runtime-execution-records`: Add the `cancel-requested` lifecycle and revise cancellation state transitions and distributed cleanup semantics.
- `executor-cancellation`: Route cancellation through the dedicated cancel request and define timeout-safe adapter cleanup behavior.

## Impact

- Affected core coordination: `src/daggerml/_core/exec_state.py`, `remote.py`, and `index.py`.
- Affected adapter and executor protocol code under `src/daggerml/contrib/`.
- Affected remote ref and execution-state schemas.
- Existing cancellation lifecycle names and adapter payloads change; this is a protocol and behavior change for adapter integrations.
- Documentation and focused execution coordination tests must be updated.
