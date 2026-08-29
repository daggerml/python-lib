## Purpose
Define remote pointer formats, CAS object publication, materialization, and liveness.

## Requirements

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

### Requirement: Push SHALL publish reachable CAS objects before writing the remote ref
The system SHALL publish a remote ref only after ensuring that the complete object graph reachable from the root typed ref is present in remote CAS. It SHALL recursively upload locally available reachable objects. If local traversal reaches a commit recorded as intentionally unavailable, non-forced branch publication SHALL proceed only when the observed existing remote branch tip was reached through available local ancestry and therefore anchors the omitted closure. Creation of a remote ref, forced publication, or publication whose existing remote tip cannot be reached before a shallow boundary SHALL fail until the history is deepened or unshallowed.

Each uploaded CAS object SHALL be a canonical JSON encoding of that object's persisted payload using the remote-private tagged serde.

#### Scenario: Push stores canonical tagged JSON CAS objects
- **WHEN** a pushed root object reaches nested DAG, node, and datum objects through typed refs
- **THEN** the system uploads every locally available missing CAS object as canonical tagged JSON
- **AND** it does not upload DB raw payloads
- **AND** it writes the remote ref only after the required CAS objects are present

#### Scenario: Update branch from shallow remote tip
- **WHEN** a local commit descends through available history from the observed existing remote branch tip and older parents are intentionally unavailable
- **THEN** non-forced push may publish the commit after uploading its available closure

#### Scenario: Reject new shallow remote root
- **WHEN** publication would create or forcibly replace a remote ref whose closure includes an intentionally unavailable local commit
- **THEN** publication fails with unshallow guidance without writing the remote ref

### Requirement: Pull SHALL materialize objects by recursive ref traversal
The system SHALL resolve `ref.to` from the remote ref payload and support complete or depth-limited project commit materialization. For each included commit it SHALL fetch the complete non-parent object closure, decode persisted payloads using expected namespaces, and continue through commit parents only as required by the selected history mode. Generic non-project object materialization SHALL remain complete and SHALL NOT apply commit-depth behavior.

Remote object materialization SHALL recompute object identity by writing each decoded object with ordinary typed storage and SHALL fail if the resulting ref does not match the expected ref. Intentionally omitted commit parents SHALL be recorded as shallow history; every other unavailable dependency SHALL fail materialization.

#### Scenario: Pull validates CAS object identity by recomputation
- **WHEN** a pulled CAS object decodes successfully
- **THEN** the system materializes it with ordinary typed object insertion
- **AND** it requires the resulting ref to equal the expected ref
- **AND** it does not force the object into place with `to=` or a raw write path

#### Scenario: Commit depth does not limit DAG closure
- **WHEN** a project commit is materialized at depth one
- **THEN** the system fetches its complete tree and DAG object closure while omitting only otherwise-unavailable commit parents

#### Scenario: Execution object materialization remains complete
- **WHEN** execution state materializes a DAG or result ref
- **THEN** every reachable object is materialized without applying project commit-history depth

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

### Requirement: Tombstones SHALL move the original ref unchanged
Tombstones SHALL continue to preserve deleted typed project refs unchanged. Plain cache-pointer deletion and execution-record cleanup SHALL use CAS and SHALL NOT require typed-ref tombstones.

#### Scenario: Cache deletion is conditional without typed tombstone
- **WHEN** a cache pointer is deleted after cancelation or invalidation
- **THEN** deletion is conditional on its ETag and execution ID
- **AND** no typed active-ref tombstone is created
