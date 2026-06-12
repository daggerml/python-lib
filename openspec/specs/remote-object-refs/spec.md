## ADDED Requirements

### Requirement: Remote refs SHALL be typed object pointers
The system SHALL encode every published remote ref as a minimal typed pointer payload containing `ref.to`, `created`, and `metadata`.

`metadata` is unconstrained globally, but specific ref families MAY require specific metadata fields.

#### Scenario: Project branch ref payload uses typed root pointer
- **WHEN** a remote project branch is published
- **THEN** the ref payload contains `ref.to = "commit:<oid>"`
- **AND** it contains integer `created`
- **AND** it contains object `metadata`

#### Scenario: Active ref payload uses typed root pointer
- **WHEN** an active execution ref is published for cache key `ck1`
- **THEN** the ref payload contains `ref.to = "node-argv:<oid>"`
- **AND** it does not require manifest closure fields

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
The system SHALL determine remote CAS liveness by traversing reachable stored objects recursively from published remote refs.

Remote liveness traversal SHALL decode remote CAS objects directly and SHALL NOT require a temporary local database only for CAS deserialization.

#### Scenario: GC traverses decoded remote objects without scratch DB
- **WHEN** remote GC marks live CAS objects from published refs
- **THEN** it decodes each visited CAS object directly from the remote tagged JSON format
- **AND** it discovers child refs from the decoded object graph
- **AND** it does not create a temporary local database only to inspect CAS payloads

### Requirement: Tombstones SHALL move the original ref unchanged
The system SHALL record tombstones by moving the original live ref payload to the tombstone location without changing its contents.

#### Scenario: Tombstone preserves deleted active ref payload
- **WHEN** `refs/active/ck1.json` is deleted
- **THEN** the tombstone payload is byte-for-byte the original active ref payload
- **AND** it still contains `ref.to = "node-argv:<oid>"`
- **AND** it still contains the original `metadata.execution_id`
