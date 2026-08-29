## ADDED Requirements

### Requirement: DAG result access raises terminal errors
The public `Dag.result` accessor SHALL raise the persisted `Error` when a committed DAG has a terminal error ref instead of a result ref.

#### Scenario: Access the result of a failed committed DAG
- **WHEN** a caller accesses `Dag.result` for a committed DAG whose description has an `error` ref and no `result` ref
- **THEN** the accessor SHALL hydrate that ref through the public DAG error query and raise the resulting `Error`

#### Scenario: Access the result of a successful committed DAG
- **WHEN** a caller accesses `Dag.result` for a committed DAG whose description has a `result` ref and no `error` ref
- **THEN** the accessor SHALL return the node represented by the result ref without hydrating or raising an error

#### Scenario: Access the result of an unfinished committed DAG
- **WHEN** a caller accesses `Dag.result` for a committed DAG whose description has neither an `error` ref nor a `result` ref
- **THEN** the accessor SHALL raise the existing repository error indicating that the DAG has not been committed yet
