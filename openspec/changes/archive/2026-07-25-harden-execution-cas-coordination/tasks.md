## 1. CAS Coordination Primitives

- [x] 1.1 Add a bounded exponential-backoff retry policy for caller execution-record child registration, rereading the record on every CAS conflict and raising on exhaustion.
- [x] 1.2 Add the same bounded retry-and-raise behavior for terminal-child bookkeeping while preserving retryable terminal coordination state.
- [x] 1.3 Add focused comments at the registration and completion coordination boundaries explaining why silent CAS exhaustion is unsafe.

## 2. Launch Failure Handling

- [x] 2.1 Update `get_or_start_fn()` to prevent adapter invocation when child registration fails.
- [x] 2.2 Roll back the failed attempt's caller/callee edge and fresh active/reservation artifacts while preserving reused shared execution artifacts.

## 3. Cancellation Lineage

- [x] 3.1 Remove `_finalize_spawned_edge()` and its cancellation-driver calls.
- [x] 3.2 Update cancellation driving to treat canceled spawned children as satisfied without removing them from `spawned_execution_ids`.
- [x] 3.3 Align execution-record and graph-facing code comments with uncompleted spawned lineage and terminal completed lineage.

## 4. Contract Coverage And Documentation

- [x] 4.1 Add contention contracts for registration-versus-cancellation CAS ordering and retry exhaustion without adapter invocation.
- [x] 4.2 Add contracts for fresh-versus-reused launch artifact rollback after registration failure.
- [x] 4.3 Add contracts for terminal-child bookkeeping retry exhaustion and safe later retry behavior.
- [x] 4.4 Add contracts that canceled children remain spawned, remain outside completed lineage, and satisfy parent cancellation driving.
- [x] 4.5 Update execution-record and execution-state documentation for bounded CAS coordination, rollback, and child-list semantics.
