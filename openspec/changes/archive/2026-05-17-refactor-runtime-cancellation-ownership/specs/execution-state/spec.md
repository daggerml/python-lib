## ADDED Requirements

### Requirement: Caller-owned launch state SHALL be serialized by cache-key lock
The runtime SHALL persist caller-owned `launch_state` for each execution attempt separately from lifecycle state. `launch_state` SHALL contain `execution_id`, `cache_key`, `resume_state`, and `created_at`. The runtime SHALL create and update `launch_state` only while holding the coordination lock for the corresponding `cache_key`.

#### Scenario: First running launch persists launch state under lock
- **WHEN** `start_fn` launches a new execution and receives a `running` adapter result with durable resume data
- **THEN** it SHALL persist `launch_state` containing `execution_id`, `cache_key`, `resume_state`, and `created_at`
- **AND** it SHALL do so while holding the lock for that `cache_key`

#### Scenario: Resume reads launch state under lock
- **WHEN** `start_fn` resumes an execution referenced by `active/<cache_key>`
- **THEN** it SHALL read that execution's `launch_state` while holding the lock for that `cache_key`
- **AND** it SHALL pass `resume_state` from `launch_state` to the adapter

### Requirement: Cancellation orphaning SHALL remove current-execution ownership under lock
When cancellation leaves an execution with no remaining live callers, the runtime SHALL acquire the coordination lock for that execution's `cache_key`, recheck that no live callers remain, ensure the execution is not terminal, and remove `active/<cache_key>` before marking cancellation intent on lifecycle state.

#### Scenario: Orphaned callee loses active pointer before cancellation lifecycle update
- **WHEN** cancellation removes the last live caller edge for callee execution `e1`
- **THEN** the runtime SHALL lock the coordination key for `e1`'s `cache_key`
- **AND** it SHALL delete `active/<cache_key>` before setting the callee lifecycle to a `cancel-*` value

#### Scenario: New caller relaunches after detached cancellation
- **WHEN** a later caller computes the same `cache_key` after the prior execution was cancellation-detached and `active/<cache_key>` is absent
- **THEN** the runtime SHALL treat the computation as having no current execution
- **AND** it SHALL create a fresh execution attempt instead of resuming the detached one
