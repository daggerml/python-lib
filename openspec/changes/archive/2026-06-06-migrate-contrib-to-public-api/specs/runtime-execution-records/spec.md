## ADDED Requirements

### Requirement: Contrib migration SHALL NOT change runtime execution-record implementation
This contrib migration SHALL rely on the existing runtime execution-record and adapter-dispatch implementation. It SHALL NOT modify runtime execution-record storage, `Dml.runtime` behavior, `IndexOps`, `ExecutionState`, adapter-envelope production, cache publication, or public API entrypoints outside contrib.

#### Scenario: Existing runtime creates execution-aware index
- **WHEN** contrib needs a worker DAG for a runtime execution
- **THEN** contrib SHALL call the existing public DAG creation path with `cache_key` and `execution_id`
- **AND** the existing runtime implementation SHALL remain responsible for materializing the active argv and maintaining execution records

#### Scenario: Runtime envelope mismatch is encountered
- **WHEN** contrib adapter code encounters a protocol mismatch during implementation
- **THEN** the mismatch SHALL be resolved inside contrib-owned parsing or normalization code if possible
- **AND** runtime/core files SHALL NOT be modified as part of this change
