## MODIFIED Requirements

### Requirement: Remote refs SHALL be typed object pointers
Published project refs SHALL remain typed pointer payloads. Execution cache pointers SHALL be plain execution IDs. Each execution's typed input root SHALL reside at `exec/execution/<execution_id>/metadata.json` field `argv_ref`, and its typed result root SHALL reside at `exec/execution/<execution_id>/state.json` field `result_ref`. Active, transport, cancel-target, and unified execution-record ref families SHALL NOT be published or interpreted.

#### Scenario: Project branch ref payload uses typed root pointer
- **WHEN** a remote project branch is published
- **THEN** the ref payload contains `ref.to = "commit:<oid>"`
- **AND** it contains integer `created`
- **AND** it contains object `metadata`

#### Scenario: Cache pointer contains execution identity only
- **WHEN** execution `e1` claims cache key `ck1`
- **THEN** `exec/cache/ck1` contains only `e1`

#### Scenario: Execution record carries typed roots
- **WHEN** execution `e1` has input and result objects
- **THEN** `exec/execution/e1/metadata.json.argv_ref` contains the typed input ref
- **AND** `exec/execution/e1/state.json.result_ref` contains the typed result ref

#### Scenario: Unified execution object is unsupported
- **WHEN** an execution ID is represented only by a unified execution object
- **THEN** it is not interpreted as a current execution record

### Requirement: Remote liveness SHALL follow the reachable object graph
Remote GC SHALL validate the exact metadata, state, and driver files for every discovered execution before deriving liveness. It SHALL treat only validated `metadata.json.argv_ref` and `state.json.result_ref` values in retained split records as object-graph roots in addition to published project refs. It SHALL preserve valid execution records reachable from cache pointers or retained lineage/control policy and SHALL collect valid unreachable losing-attempt records according to that policy. A partial, malformed, extra-field, extra-file, or unified execution shape SHALL fail validation and SHALL NOT be parsed, migrated, tolerated, or specially preserved.

#### Scenario: Current running execution keeps argv live
- **WHEN** `exec/cache/ck1` contains `e1` and valid `exec/execution/e1/metadata.json.argv_ref` names an argv root
- **THEN** remote GC preserves the argv object closure

#### Scenario: Terminal execution keeps result live
- **WHEN** a retained valid split execution has a non-null `state.json.result_ref`
- **THEN** remote GC preserves the result DAG closure

#### Scenario: Lost reservation can be collected
- **WHEN** a valid split execution record is not reachable from cache pointers, lineage, or retained control state
- **THEN** remote GC MAY collect that record and its otherwise unreachable roots

#### Scenario: Partial execution fails closed
- **WHEN** any required split execution file is absent or malformed
- **THEN** remote GC fails validation before deleting CAS based on that execution's roots

#### Scenario: Unified execution is not preserved
- **WHEN** remote GC encounters an unsupported unified execution object
- **THEN** it does not parse or retain that object as compatible execution state
