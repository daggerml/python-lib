## Why

Remote execution coordination is currently spread across lock, active, launch, lifecycle, transport, cancel-target, cache, and invalidation objects. Consolidating attempt-owned state into one CAS-updated execution record makes parallel callers across machines easier to coordinate and removes multi-object state transitions that can become inconsistent.

## What Changes

- **BREAKING** Replace the existing remote execution layout with `cache/<cache_key>` plain execution-ID pointers and unified `execution/<execution_id>` records.
- Store lifecycle, lock, adapter state, argv and result refs, lineage summaries, cancelation state, and invalidation state in each execution record; retain separate call-edge and adapter `io/<execution_id>/` objects.
- Make the execution lock owner the only actor allowed to mutate an execution record, with acquisition, mutation, and release guarded by S3 compare-and-swap.
- Determine lease expiry from S3 response time using `LastModified + lock.ttl <= Date`; permit an expired owner to continue unless another owner successfully steals the lock.
- Create an execution record before conditionally creating its cache pointer; remove the new record after a lost pointer race and reread the winner.
- Persist adapter state after every adapter call, resume expired executions by execution ID, and require repeated adapter calls to act as idempotent status checks.
- Store completed or error DAG identity directly as `result_ref`, replacing transport refs; cache pointers remain bound from reservation until CAS deletion by cancelation or invalidation.
- Remove compatibility with the previous layout. This is an intentional v0 storage break with no migration path.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `execution-state`: Replace separate cache-key locks and state objects with owner-only mutation of unified execution records and S3-time lease expiry.
- `runtime-execution-records`: Define the unified record schema, cache-pointer invariant, creation race, result publication, resume, and terminal behavior.
- `execution-admin-controls`: Move cancelation and invalidation state into locked execution records and conditionally delete current cache pointers.
- `execution-call-edges`: Preserve separate edge objects while adapting registration cleanup to unified execution records and cache pointers.
- `remote-object-refs`: Remove active, transport, and cancel-target ref families and make execution-record argv/result refs roots for remote object liveness.
- `adapter-operation-protocol`: Rename resume state to adapter state, persist it after every call, and define idempotent repeated status checks with success, retry, and error outcomes.
- `executor-cancellation`: Remove CloudFormation executor cancellation requirements after deleting that executor.
- `contrib-public-api-migration`: Remove the CloudFormation worker scenario and require unified nested adapter field names.

## Impact

- Major changes to `src/daggerml/_core/exec_state.py`, `remote.py`, `s3_cas.py`, `index.py`, and their contract/integration tests.
- Adapter and executor envelopes change from resume-state semantics to adapter-state status checks.
- Remote GC must trace `argv_ref` and `result_ref` from execution records.
- Human-facing execution, cache, runtime-state, adapter, and remote architecture documentation must describe the new incompatible layout.
- No backward compatibility, dual writes, migration, or legacy reads are required.
