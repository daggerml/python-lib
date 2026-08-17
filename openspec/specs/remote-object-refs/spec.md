## Purpose
Define remote pointer formats, CAS object publication, materialization, and liveness.

## Requirements

### Requirement: Remote refs SHALL be typed object pointers
Published project refs SHALL remain typed pointer payloads. Execution cache pointers SHALL instead be plain execution IDs, while typed `argv_ref` and `result_ref` values SHALL reside in unified execution records. Active, transport, and cancel-target ref families SHALL NOT be published.

#### Scenario: Project branch ref payload uses typed root pointer
- **WHEN** a remote project branch is published
- **THEN** the ref payload contains `ref.to = "commit:<oid>"`
- **AND** it contains integer `created`
- **AND** it contains object `metadata`

#### Scenario: Cache pointer contains execution identity only
- **WHEN** execution `e1` claims cache key `ck1`
- **THEN** `cache/ck1` contains only `e1`

#### Scenario: Execution record carries typed roots
- **WHEN** `execution/e1` has input and result objects
- **THEN** its `argv_ref` and `result_ref` contain typed DaggerML refs

### Requirement: Push SHALL publish reachable CAS objects before writing the remote ref
The system SHALL publish a remote ref only after recursively traversing the local object graph from the root typed ref and uploading any reachable CAS objects missing on the remote.

Each uploaded CAS object SHALL be a canonical JSON encoding of that object's persisted payload using the remote-private tagged serde.

#### Scenario: Push stores canonical tagged JSON CAS objects
- **WHEN** a pushed root object reaches nested DAG, node, and datum objects through typed refs
- **THEN** the system uploads every missing reachable CAS object as canonical tagged JSON
- **AND** it does not upload DB raw payloads
- **AND** it writes the remote ref after the required CAS objects are present

### Requirement: Pull SHALL materialize objects by recursive ref traversal
The system SHALL resolve `ref.to` from the remote ref payload, fetch the referenced CAS object if absent locally, decode its persisted payload using the expected root namespace from that ref, and continue recursively until the reachable graph is materialized locally.

Remote object materialization SHALL recompute object identity by writing the decoded object with ordinary typed storage and SHALL fail if the resulting ref does not match the expected ref.

#### Scenario: Pull validates CAS object identity by recomputation
- **WHEN** a pulled CAS object decodes successfully
- **THEN** the system materializes it with ordinary typed object insertion
- **AND** it requires the resulting ref to equal the expected ref
- **AND** it does not force the object into place with `to=` or a raw write path

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
