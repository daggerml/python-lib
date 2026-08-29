## 1. Protocol And Remote Refs

- [x] 1.1 Define typed `AdapterInvokeRequest` / `AdapterInvokeResponse` and `AdapterCancelRequest` / `AdapterCancelResponse` contracts.
- [x] 1.2 Add remote ref helpers for `refs/cancel-targets/<execution_id>.json` using the existing argv manifest without regeneration.
- [x] 1.3 Implement conditional active-ref movement that verifies the source execution ID and preserves the manifest CAS closure.
- [x] 1.4 Update adapter CLI parsing, dispatch, and executor bridging for separate invoke and cancel operations.

## 2. Phase 1 Cancellation Planning

- [x] 2.1 Replace the current cancellation planner with a user-seeded execution-ID work set.
- [x] 2.2 Process each execution under its cache-key lock, retrying lock acquisition and rechecking live callers before lifecycle changes.
- [x] 2.3 Add the `cancel-requested` lifecycle and persist it before moving the active ref to the cancel target.
- [x] 2.4 Enqueue direct callees and release each execution lock before processing the next work item.
- [x] 2.5 Make Phase 1 idempotent and ensure it performs no adapter calls.

## 3. Phase 2 Distributed Cleanup

- [x] 3.1 Implement distributed leaf-first cleanup for `cancel-requested` executions.
- [x] 3.2 Wait for direct callees to become `cancel-ready` before dispatching their cancel operations.
- [x] 3.3 Build `AdapterCancelRequest` from the execution-owned cancel-target ref and launch state.
- [x] 3.4 Persist callee `canceled` state and advance the current execution to `cancel-ready` under conditional lifecycle updates.
- [x] 3.5 Track the 60-second `cancel-ready` timeout and run fallback cancel adapters when the handoff expires.
- [x] 3.6 Make normal and timeout-driven cancellation claims and executor cleanup safe to retry.
- [x] 3.7 Reap cancel-target refs only after runtime-owned cleanup handling completes.

## 4. Invocation And Runtime Integration

- [x] 4.1 Update invoke dispatch to use `AdapterInvokeRequest` without cancellation-only fields.
- [x] 4.2 Preserve existing start/poll resume behavior and launch-state persistence for invoke operations.
- [x] 4.3 Verify mutation and activation lifecycle guards reject the new cancellation lifecycle through the existing execution-runtime path.
- [x] 4.4 Preserve caller/callee lineage semantics and ensure canceled descendants are not recorded as completed children.

## 5. Documentation And Tests

- [x] 5.1 Update execution lifecycle, remote protocol, and adapter runtime documentation to describe the two phases and actual S3 schemas.
- [x] 5.2 Remove `argv_ptr` from invoke-envelope documentation and document it only on the cancel request.
- [x] 5.3 Add tests for active-ref movement, conditional rebinding protection, and cancel-target materialization.
- [x] 5.4 Add tests for Phase 1 work-set traversal, live-caller stopping, lock retry, and no-adapter-call behavior.
- [x] 5.5 Add tests for distributed leaf-first Phase 2 ordering, lifecycle transitions, timeout fallback, and retry safety.
- [x] 5.6 Add adapter contract tests covering separate invoke/cancel requests and responses.
