## ADDED Requirements

### Requirement: Projection values SHALL be normalized by a built-in codec
The codec registry SHALL include a built-in codec that recognizes `Projection` values and converts projections from the active target DAG's `Dml` instance into refs belonging to that active DAG.

#### Scenario: Built-in registrations include the projection codec
- **WHEN** the public API loads its built-in codec registrations
- **THEN** a codec capable of encoding `Projection` values is registered alongside the existing node and Python-type codecs

#### Scenario: Recursive normalization encounters a projection
- **WHEN** `apply_codecs()` encounters a same-`Dml` projection directly or nested in another supported value
- **THEN** it dispatches the projection through the built-in projection codec

### Requirement: Projection encoding SHALL import the base and replay access steps
The projection codec SHALL insert the projection's committed base node into the active target DAG as an import and SHALL insert one builtin `get` access node for each stored projection path step in order. It SHALL return the final active-DAG node ref to the calling normalization path.

#### Scenario: Nested dictionary projection is encoded
- **WHEN** a projection has base node `root` and path `("my_key", "my_key1")`
- **THEN** encoding inserts an import of `root`
- **AND** encoding inserts `get(imported_root, "my_key")` followed by `get(previous_result, "my_key1")`
- **AND** encoding returns the second access node ref

#### Scenario: List index or slice projection is encoded
- **WHEN** a projection path contains an integer index or normalized slice bounds
- **THEN** the codec passes each stored step unchanged to the corresponding builtin `get` access node

#### Scenario: Named direct put binds the final access node
- **WHEN** `Dag.put(projection, name=...)` completes codec normalization
- **THEN** normal literal staging binds the supplied name to the final projection access ref
- **AND** it does not materialize the projected Python value as a copied literal
