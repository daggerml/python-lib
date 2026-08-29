## 1. Persistence Model Refactor

- [x] 1.1 Split the current execution persistence helpers into caller-owned `launch_state` and runtime-owned `execution_record` read/write paths.
- [x] 1.2 Update cache-key lock and `active/<cache_key>` handling so launch/resume flows use `launch_state` and orphan-triggered cancellation clears the active pointer before lifecycle cancellation updates.
- [x] 1.3 Rename lifecycle fields and values across runtime types and validation logic to use `lifecycle`, `spawned_execution_ids`, `cancellation_requested_by`, `cancel-pending`, and `cancel-detached`.

## 2. Runtime and Cancellation Flow

- [x] 2.1 Refactor `IndexOps.start_fn` so caller-owned launch/resume state and runtime-owned lifecycle state are updated through their new ownership boundaries.
- [x] 2.2 Refactor cancellation planning and execution so live caller edges are caller-owned, orphan detection uses those live edges, and detached cancellation uses CAS on `execution_record` plus cancellation tombstones.
- [x] 2.3 Update `dml.runtime.cancel(index_id)` to run as an out-of-band workflow that records `config.user` when no active caller execution context exists and raises `CancelledExecutionError` on cancellation interruption.

## 3. Executors and Adapter Contract

- [x] 3.1 Update adapter envelope/result validation and executor dispatch to use the renamed cancellation lifecycle values while preserving the envelope field names required by the contract.
- [x] 3.2 Update built-in executors to treat `cancel-pending` as the cancellation update signal and return `cancel-detached` after successful fire-and-forget cancellation handling.

## 4. Contract Coverage and Documentation

- [x] 4.1 Update runtime, index-ops, and executor contract tests for the split persistence model, ETag drift behavior, direct user cancellation requester behavior, and detached cancellation semantics.
- [x] 4.2 Update the runtime and executor documentation to reflect `launch_state`, `execution_record`, live caller edge ownership, spawned execution summaries, and the accepted best-effort cancellation limitation through terminal intermediates.
