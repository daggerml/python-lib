## 1. Execution Reservation

- [x] 1.1 Add `pending` to the execution-record lifecycle model for reserved adapter-backed child executions.
- [x] 1.2 Make `ExecutionState` create the child execution record before publishing or reusing `active/<cache_key>`.
- [x] 1.3 Ensure stale active-pointer recovery treats a missing execution record as stale state rather than a normal launch window.

## 2. Execution Activation

- [x] 2.1 Make execution-aware `IndexOps.create(cache_key, execution_id)` require an existing reserved execution record.
- [x] 2.2 Transition execution-aware activation from `pending` to `running` when local index creation succeeds.
- [x] 2.3 Reject `running`, `succeeded`, `failed`, and missing execution records as invalid activation attempts.

## 3. Cancellation Gates

- [x] 3.1 Treat execution-aware `IndexOps.create` as a mutation operation for cancellation handling.
- [x] 3.2 When execution-aware `create` sees `cancel-pending`, run `cancel(mode="drive")` outside local mutation work and then raise `CancellationError`.
- [x] 3.3 When execution-aware `create` sees `cancel-ready` or `canceled`, raise `CancellationError` without local mutation.

## 4. Verification

- [x] 4.1 Add or update execution-state tests for pending reservation and active-pointer ordering.
- [x] 4.2 Add or update index/runtime tests for execution-aware activation lifecycle transitions.
- [x] 4.3 Add or update cancellation-gate tests for execution-aware `create` handling of `cancel-pending`, `cancel-ready`, and invalid non-pending states.
