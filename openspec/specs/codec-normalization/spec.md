## Purpose
Define the Dag-owned Python codec contract, plugin discovery, recursive normalization, and built-in projection behavior.

## Requirements

### Requirement: Codecs receive Dag instances
The codec plugin contract SHALL pass the active `daggerml.api.Dag` into codec `encode(...)`. `CodecContext` and staged compatibility call semantics SHALL NOT be part of the current codec contract.

#### Scenario: Built-in codec receives Dag
- **WHEN** a built-in codec encodes a value
- **THEN** its `encode(...)` method receives the active `Dag` instance

#### Scenario: Plugin codec receives Dag
- **WHEN** a plugin codec loaded from the `daggerml.codecs` entry-point group encodes a value
- **THEN** its `encode(...)` method receives the active `Dag` instance

### Requirement: Dag owns recursive codec normalization
`daggerml.api.Dag` SHALL own recursive codec normalization and insertion for values accepted by public staging and call-entry methods.

#### Scenario: Dag.put normalizes recursively before runtime staging
- **WHEN** `Dag.put(value)` is called
- **THEN** `Dag` recursively applies codecs and normalizes nested values before delegating to runtime literal staging

#### Scenario: Dag.call inserts callable and arguments before execution
- **WHEN** `Dag.call(fn, *args, **kwargs)` is called
- **THEN** `Dag` inserts the callable, positional arguments, and keyword argument values through the codec-driven normalization path before invoking runtime function staging

#### Scenario: Node remains a codec during Dag-owned normalization
- **WHEN** a `Node` value is encountered during normalization
- **THEN** the system handles it through the built-in `Node` codec rather than through a special non-codec rule

### Requirement: Codec plugins remain discoverable through the existing entry-point group
The system SHALL load codec plugins from the `daggerml.codecs` entry-point group using the sole current Dag-owned codec contract.

#### Scenario: Entry-point group remains stable
- **WHEN** codec plugins are discovered
- **THEN** discovery uses the `daggerml.codecs` entry-point group
- **AND** plugin loading preserves deterministic ordering and re-encode behavior

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
