## MODIFIED Requirements

### Requirement: Remote refs SHALL be typed object pointers
Published project refs SHALL remain typed pointer payloads. Execution cache pointers SHALL instead be plain execution IDs, while typed `argv_ref` and `result_ref` values SHALL reside in unified execution records. Active, transport, and cancel-target ref families SHALL NOT be published.

#### Scenario: Cache pointer contains execution identity only
- **WHEN** execution `e1` claims cache key `ck1`
- **THEN** `cache/ck1` contains only `e1`

#### Scenario: Execution record carries typed roots
- **WHEN** `execution/e1` has input and result objects
- **THEN** its `argv_ref` and `result_ref` contain typed DaggerML refs

### Requirement: Remote liveness SHALL follow the reachable object graph
Remote GC SHALL treat typed `argv_ref` and `result_ref` values in retained execution records as object-graph roots in addition to published project refs. It SHALL preserve execution records reachable from cache pointers or retained lineage/control policy and SHALL collect unreachable losing-attempt records according to that policy.

#### Scenario: Current running execution keeps argv live
- **WHEN** `cache/ck1` contains `e1` and `execution/e1.argv_ref` names an argv root
- **THEN** remote GC preserves the argv object closure

#### Scenario: Terminal execution keeps result live
- **WHEN** a retained execution has a non-null `result_ref`
- **THEN** remote GC preserves the result DAG closure

#### Scenario: Lost reservation can be collected
- **WHEN** an execution record is not reachable from cache pointers, lineage, or retained control state
- **THEN** remote GC MAY collect that record and its otherwise unreachable roots

### Requirement: Tombstones SHALL move the original ref unchanged
Tombstones SHALL continue to preserve deleted typed project refs unchanged. Plain cache-pointer deletion and execution-record cleanup SHALL use CAS and SHALL NOT require typed-ref tombstones.

#### Scenario: Cache deletion is conditional without typed tombstone
- **WHEN** a cache pointer is deleted after cancelation or invalidation
- **THEN** deletion is conditional on its ETag and execution ID
- **AND** no typed active-ref tombstone is created
