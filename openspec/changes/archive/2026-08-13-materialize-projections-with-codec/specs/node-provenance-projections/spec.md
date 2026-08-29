## MODIFIED Requirements

### Requirement: `Projection` SHALL support read-only node-like interrogation only
`Projection` SHALL support read-only interrogation helpers equivalent to committed-node inspection, including `.value()`, `.context(root=...)`, nested indexing, `type`, `keys()`, iteration, and length. It SHALL NOT have independent ref-based identity, mutable-node helpers, or callable behavior, but it SHALL be accepted by codec normalization so its committed base and access path can be materialized in an active DAG from the same `Dml` instance.

#### Scenario: Projection value materializes selected subvalue
- **WHEN** a caller invokes `.value()` on a `Projection`
- **THEN** the API materializes the selected subvalue by reading the base committed value and applying the stored projection path

#### Scenario: Projection rejects direct mutable and callable semantics
- **WHEN** a caller attempts to mutate or invoke a `Projection` directly
- **THEN** the API rejects that operation rather than treating the projection as a persisted node

#### Scenario: Projection is materialized through codec normalization
- **WHEN** a caller supplies a `Projection` from the same `Dml` instance to a codec-normalized input of an active DAG
- **THEN** the system inserts its committed base node and access path into the active DAG
- **AND** the source committed DAG remains unchanged
