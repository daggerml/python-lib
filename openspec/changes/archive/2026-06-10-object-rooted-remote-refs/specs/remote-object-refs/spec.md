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

#### Scenario: Push uploads missing nested objects without special manifest layers
- **WHEN** a pushed root object reaches nested DAG, node, and datum objects through typed refs
- **THEN** the system uploads every missing reachable CAS object
- **AND** it does not require special handling for `commit` or `dag` subobjects
- **AND** it writes the remote ref after the required CAS objects are present

### Requirement: Pull SHALL materialize objects by recursive ref traversal
The system SHALL resolve `ref.to` from the remote ref payload, fetch the referenced CAS object if absent locally, decode its direct typed refs, and continue recursively until the reachable graph is materialized locally.

#### Scenario: Pull stops when a reachable object already exists locally
- **WHEN** a pulled object graph reaches a typed ref whose object is already present in the local DB
- **THEN** the system reuses that local object
- **AND** it does not require a manifest closure to continue or stop traversal

### Requirement: Remote liveness SHALL follow the reachable object graph
The system SHALL determine remote CAS liveness by traversing reachable stored objects recursively from published remote refs.

#### Scenario: GC keeps nested object reachable through stored refs
- **WHEN** a published remote ref reaches a nested datum through commit, tree, DAG, and node objects
- **THEN** remote GC retains that datum CAS object
- **AND** it does not depend on a synthetic closure field stored beside the ref

### Requirement: Tombstones SHALL move the original ref unchanged
The system SHALL record tombstones by moving the original live ref payload to the tombstone location without changing its contents.

#### Scenario: Tombstone preserves deleted active ref payload
- **WHEN** `refs/active/ck1.json` is deleted
- **THEN** the tombstone payload is byte-for-byte the original active ref payload
- **AND** it still contains `ref.to = "node-argv:<oid>"`
- **AND** it still contains the original `metadata.execution_id`
