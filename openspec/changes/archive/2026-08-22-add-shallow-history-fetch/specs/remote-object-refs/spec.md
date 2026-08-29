## MODIFIED Requirements

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
