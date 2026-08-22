## MODIFIED Requirements

### Requirement: Cancellation Phase 2 SHALL cancel the selected set directly
Phase 2 SHALL begin only after Phase 1 has determined the complete cancellation set. In each attempt round it SHALL invoke cancel adapters for every remaining selected execution concurrently and wait for the round to settle. Each invocation SHALL respect the execution's persisted `driver.not_before`, acquire its execution coordination lock before rereading adapter inputs, hold that lock across the adapter call and response persistence, and release it in every outcome. It SHALL transition an execution from `cancel-pending` to `canceled` only after successful cancellation, retry only unsuccessful executions, and stop after all succeed or the configured retry budget is exhausted. Unsuccessful executions SHALL remain `cancel-pending` and exhaustion SHALL be observable. Executions already observed as `cancel-pending` SHALL be eligible for resumed planning and Phase 2 processing.

#### Scenario: Selected adapter work advances directly to canceled
- **WHEN** Phase 2 receives successful cancellation for a `cancel-pending` execution
- **THEN** it SHALL compare-and-swap that execution directly to `canceled`

#### Scenario: Attempt round is concurrent and complete
- **WHEN** multiple selected executions remain `cancel-pending`
- **THEN** Phase 2 SHALL attempt all of them concurrently and collect every outcome before starting another round

#### Scenario: Retry targets only unsuccessful work
- **WHEN** one execution succeeds and another fails in an attempt round
- **THEN** the next round SHALL retry only the unsuccessful execution

#### Scenario: Retry waits for persisted deadline
- **WHEN** cancellation retry state contains a future `driver.not_before`
- **THEN** Phase 2 SHALL not invoke that execution's adapter before the deadline

#### Scenario: Adapter invocation is serialized per execution
- **WHEN** concurrent cancellation drivers target the same execution
- **THEN** only the driver holding that execution's coordination lock SHALL invoke its adapter
- **AND** it SHALL release the lock after persisting the outcome or encountering an error

#### Scenario: Retry exhaustion preserves pending state
- **WHEN** an execution remains unsuccessful after the initial attempt and configured retry rounds
- **THEN** it SHALL remain `cancel-pending` and Phase 2 SHALL surface exhaustion

#### Scenario: Interrupted planning is resumable
- **WHEN** a cancellation attempt stops after persisting `cancel-pending`
- **THEN** a later cancellation call SHALL reconstruct the reachable selected work and retry Phase 2

#### Scenario: Phase 2 completion conflicts
- **WHEN** the compare-and-swap from `cancel-pending` to `canceled` conflicts
- **THEN** Phase 2 SHALL reread the execution record
- **AND** it SHALL accept an already-terminal lifecycle or retry an execution that remains `cancel-pending`
