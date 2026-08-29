## MODIFIED Requirements

### Requirement: Cancellation Phase 1 SHALL not invoke adapters
Phase 1 SHALL determine the complete cancellation set before Phase 2 begins. For each reachable execution, it SHALL read lifecycle and valid caller references while holding the execution driver lock. It SHALL skip `succeeded`, `failed`, and `canceled` executions. It SHALL skip a `pending` or `running` execution that retains a valid caller reference. It SHALL reconstruct an existing `cancel-pending` execution into the selected set without rewriting its lifecycle or requiring incoming edges to be absent. It SHALL use compare-and-swap to transition only `pending` or `running` unreferenced executions to `cancel-pending`; after any conflict it SHALL reread lifecycle and caller references before retrying. After selecting an execution, Phase 1 SHALL conditionally delete its matching cache pointer, enqueue its spawned executions, and idempotently remove its outgoing caller edges. It SHALL perform no adapter invocation.

#### Scenario: Planning completes before adapter work
- **WHEN** Phase 1 processes a rooted cancellation work set
- **THEN** it SHALL determine every reachable execution selected as `cancel-pending` before Phase 2 begins
- **AND** no invoke or cancel adapter operation SHALL be sent during Phase 1

#### Scenario: Active unreferenced execution is selected
- **WHEN** a reachable unreferenced execution has lifecycle `pending` or `running`
- **THEN** Phase 1 SHALL CAS-transition it to `cancel-pending`

#### Scenario: Existing cancel-pending execution is reconstructed
- **WHEN** a reachable execution already has lifecycle `cancel-pending`
- **THEN** Phase 1 SHALL include it in the selected set without rewriting state
- **AND** it SHALL resume idempotent selected-record cleanup without applying the active-record incoming-edge gate

#### Scenario: Terminal execution is skipped
- **WHEN** a reachable execution has lifecycle `succeeded`, `failed`, or `canceled`
- **THEN** Phase 1 SHALL leave its state unchanged and continue planning

#### Scenario: Referenced execution is not selected
- **WHEN** a reachable `pending` or `running` execution retains at least one valid caller reference
- **THEN** Phase 1 SHALL leave its lifecycle unchanged
- **AND** it SHALL NOT traverse or remove that execution's outgoing caller edges as part of that branch

#### Scenario: Contention restarts complete selection decision
- **WHEN** the `cancel-pending` state CAS conflicts
- **THEN** Phase 1 SHALL reread both lifecycle and incoming caller references
- **AND** it SHALL retry only when the execution remains active and unreferenced

#### Scenario: Selected execution relinquishes dependencies
- **WHEN** Phase 1 successfully selects an execution as `cancel-pending`
- **THEN** it SHALL enqueue every execution in its `spawned_execution_ids`
- **AND** it SHALL idempotently remove every caller edge owned by that execution
- **AND** it SHALL conditionally delete the cache pointer only when it still names that execution

### Requirement: Cancellation Phase 2 SHALL cancel the selected set directly
Phase 2 SHALL begin only after Phase 1 has determined the complete cancellation set. In each attempt round it SHALL process every remaining selected execution concurrently and wait for the round to settle. It SHALL acquire each execution's driver lock before rereading lifecycle and adapter inputs. Only lifecycle `cancel-pending` SHALL enter the cancel adapter path, and cancellation SHALL respect any persisted `driver.not_before` delay. Lifecycle `canceled` SHALL be accepted and dropped as concurrent completion; `pending`, `running`, `succeeded`, or `failed` SHALL emit a warning containing execution ID and lifecycle and then be dropped without adapter invocation. A successful cancel response SHALL CAS-transition only `cancel-pending` to `canceled`. Retry or failure SHALL preserve `cancel-pending`, persist permitted driver continuation state, and remain eligible for bounded retry.

#### Scenario: Selected adapter work advances directly to canceled
- **WHEN** Phase 2 receives successful cancellation for a `cancel-pending` execution
- **THEN** it SHALL compare-and-swap that execution directly to `canceled`

#### Scenario: Attempt round is concurrent and complete
- **WHEN** multiple selected executions remain
- **THEN** Phase 2 SHALL process all of them concurrently and collect every outcome before starting another round

#### Scenario: Already canceled execution is complete
- **WHEN** Phase 2 rereads lifecycle `canceled`
- **THEN** it SHALL drop the execution without warning or adapter invocation

#### Scenario: Unexpected lifecycle is warned and dropped
- **WHEN** Phase 2 rereads lifecycle `pending`, `running`, `succeeded`, or `failed`
- **THEN** it SHALL emit a warning identifying the execution and lifecycle
- **AND** it SHALL drop the execution without invoking the cancel adapter

#### Scenario: Retry targets only unsuccessful cancel-pending work
- **WHEN** one cancel adapter succeeds and another returns retry or failure
- **THEN** the next round SHALL retry only the unsuccessful `cancel-pending` execution

#### Scenario: Cancellation respects persisted deadline
- **WHEN** selected cancellation work contains a future `driver.not_before`
- **THEN** Phase 2 SHALL not invoke its cancel adapter before that deadline

#### Scenario: Retry exhaustion preserves pending state
- **WHEN** an execution remains unsuccessful after the initial attempt and configured retry rounds
- **THEN** it SHALL remain `cancel-pending` and Phase 2 SHALL surface exhaustion

#### Scenario: Completion CAS conflict is reevaluated
- **WHEN** the `cancel-pending -> canceled` CAS conflicts
- **THEN** Phase 2 SHALL reread lifecycle
- **AND** it SHALL accept `canceled`, retry `cancel-pending`, or warn and drop any other lifecycle

### Requirement: ExecutionState SHALL expose a public mutation lifecycle guard
The runtime SHALL expose a canonical mutation guard that reads `state.json`, classifies lifecycle for activation or general index mutation, and returns current semantic state or raises a typed execution-status error. Activation SHALL accept only `pending`; general index mutation SHALL accept only `running`; and `cancel-pending` or `canceled` SHALL raise `CanceledExecutionError`. This guard SHALL NOT require the driver lock merely to read state. The separate lock-free terminal-child bookkeeping operation MAY update only lineage arrays for a `cancel-pending` caller before surfacing cancellation.

#### Scenario: Activation accepts pending state
- **WHEN** the activation guard reads lifecycle `pending`
- **THEN** it SHALL return current semantic state

#### Scenario: Mutation accepts running state
- **WHEN** the general mutation guard reads lifecycle `running`
- **THEN** it SHALL return current semantic state

#### Scenario: Cancel-pending blocks general mutation
- **WHEN** either guard reads lifecycle `cancel-pending`
- **THEN** it SHALL raise `CanceledExecutionError`
- **AND** it SHALL not persist the requested general mutation

#### Scenario: Canceled blocks mutation
- **WHEN** either guard reads lifecycle `canceled`
- **THEN** it SHALL raise `CanceledExecutionError` without invoking adapter cancellation

#### Scenario: Terminal-child bookkeeping is narrow exception
- **WHEN** terminal-child bookkeeping reads a `cancel-pending` caller
- **THEN** it MAY change only `spawned_execution_ids`, `child_execution_ids`, and derived `updated_at`
- **AND** it SHALL then surface cancellation

#### Scenario: Other non-active states reject mutation
- **WHEN** activation or general mutation reads an otherwise unsupported lifecycle
- **THEN** it SHALL raise `BadExecutionStatusError`

### Requirement: Driver mutations SHALL be serialized by owner locks
Each `driver.json` SHALL contain a nullable owner lock. Lock acquisition SHALL use compare-and-swap to replace a null or expired lock with a fresh UUID4 owner. Every adapter invocation and `driver.json` mutation other than acquisition SHALL require the current owner and compare-and-swap against the latest driver object. Every lifecycle or control mutation in `state.json` SHALL also require and verify the current driver owner before its guarded state CAS. Result publication and caller lineage summary mutations SHALL use guarded state CAS without requiring this lock. Unlock SHALL clear the lock only when the stored owner matches.

#### Scenario: One driver acquires an unlocked execution
- **WHEN** two callers concurrently attempt to acquire the null driver lock
- **THEN** exactly one conditional update SHALL succeed

#### Scenario: Funk publication proceeds during adapter ownership
- **WHEN** one caller holds the driver lock during an adapter call
- **THEN** the funk runtime MAY still publish a valid result to running state

#### Scenario: Caller lineage proceeds without caller lock
- **WHEN** a running caller registers or completes a direct child
- **THEN** its guarded lineage CAS SHALL not require the caller driver lock

#### Scenario: Lifecycle writer verifies owner
- **WHEN** a writer attempts a lifecycle or control state CAS
- **THEN** it SHALL verify the latest driver owner matches its owner token

#### Scenario: Stale driver cannot persist adapter response
- **WHEN** an adapter call returns after another owner has stolen the lock
- **THEN** the stale caller SHALL not persist adapter, retry, lifecycle, or control state

#### Scenario: Stale unlock preserves replacement owner
- **WHEN** owner `o1` attempts to unlock after owner `o2` steals the lock
- **THEN** the runtime SHALL not clear `o2`'s lock

### Requirement: Shared retry delay SHALL coordinate adapter backpressure
An adapter `retry` response MAY include nonnegative `retry_after_ms`. The current driver owner SHALL persist `not_before` as a shared absolute timestamp derived from that delay, or from the runtime's standard retry delay when the hint is absent. Before invoke, cleanup, or cancellation, every caller SHALL acquire the driver lock, reread state and driver, and skip the adapter call while `not_before` remains in the future.

#### Scenario: Backpressure delays adapter callers
- **WHEN** one invoke returns retry with `retry_after_ms = 5000`
- **THEN** the owner SHALL persist a shared not-before timestamp
- **AND** other callers SHALL not invoke, clean up, or cancel that execution before it expires

#### Scenario: Current state determines delayed operation
- **WHEN** not-before expires
- **THEN** the next owner SHALL derive invoke or cleanup from current result and cleanup state
- **AND** no operation discriminator SHALL be stored with the delay

#### Scenario: Cancellation respects delay
- **WHEN** cancellation selects an execution whose not-before is in the future
- **THEN** cancellation coordination SHALL wait until that timestamp before invoking cancel

## ADDED Requirements

### Requirement: State CAS authority SHALL be validated on every attempt
Every execution-state CAS retry SHALL reread the latest state, reevaluate the operation's allowed source lifecycle, and confirm whether the changed fields require the current driver lock. CAS exhaustion SHALL remain observable. A stale writer SHALL not restore an earlier lifecycle or mutate an absorbing terminal lifecycle.

#### Scenario: Stale activation cannot overwrite cancellation
- **WHEN** activation loses a CAS race to `cancel-pending`
- **THEN** its retry SHALL reject `cancel-pending -> running`

#### Scenario: Stale completion cannot overwrite cancellation
- **WHEN** result finalization or adapter failure loses a CAS race to `cancel-pending`
- **THEN** its retry SHALL not write `succeeded` or `failed`

#### Scenario: Terminal lifecycle cannot be reopened
- **WHEN** any writer rereads `succeeded`, `failed`, or `canceled`
- **THEN** it SHALL not change lifecycle
