## ADDED Requirements

### Requirement: Committed DAG collection traversal SHALL return read-only projections when no persisted node exists for the selected subvalue
The public API SHALL expose a read-only `Projection` object for committed-DAG dict/list traversal whenever a selected subvalue does not correspond to a standalone persisted node ref.

#### Scenario: Dict projection from committed DAG result
- **WHEN** a caller reads `loaded.result["foo"]` from a committed dict-valued DAG result and that selected subvalue has no standalone persisted node ref
- **THEN** the API returns a `Projection` instead of staging a new builtin node

#### Scenario: Nested projection composes path traversal
- **WHEN** a caller performs nested committed-DAG traversal such as `loaded.result["outer"][0]["inner"]`
- **THEN** each step extends the read-only projection path without mutating repository state

### Requirement: `Projection` SHALL support read-only node-like interrogation only
`Projection` SHALL support read-only interrogation helpers equivalent to committed-node inspection, including `.value()`, `.context(root=...)`, nested indexing, `type`, `keys()`, iteration, and length, and it SHALL NOT support mutation, callable behavior, codec insertion, or ref-based identity semantics.

#### Scenario: Projection value materializes selected subvalue
- **WHEN** a caller invokes `.value()` on a `Projection`
- **THEN** the API materializes the selected subvalue by reading the base committed value and applying the stored projection path

#### Scenario: Projection rejects write-style semantics
- **WHEN** a caller attempts to use a `Projection` as a mutable node, callable node, or ref-bearing runtime input
- **THEN** the API rejects that operation rather than treating the projection as a persisted node

### Requirement: `context(root=False)` SHALL return the nearest non-builtin provenance context for projected or builtin-derived values
For both real `Node` values and `Projection` values, `context(root=False)` SHALL backtrack through builtin-produced structure and builtin selection paths until it reaches the first non-builtin function/import provenance boundary, and it SHALL return that boundary's DAG.

#### Scenario: Projection context returns nearest function DAG
- **WHEN** a committed imported result contains `{"foo": <fn result>}` and a caller evaluates `result["foo"].context(root=False)`
- **THEN** the API returns the function DAG that produced the `"foo"` value rather than a builtin collection or builtin selection DAG

#### Scenario: Builtin-derived open-DAG node skips builtin context
- **WHEN** a caller evaluates `context(root=False)` on a real node produced by builtin collection selection such as `c["a"]`
- **THEN** the API backtracks through builtin provenance and returns the nearest non-builtin provenance context behind the selected value

### Requirement: `context(root=True)` SHALL recurse until provenance no longer crosses a non-builtin function/import boundary
For both real `Node` values and `Projection` values, `context(root=True)` SHALL repeatedly resolve nearest non-builtin import/function provenance boundaries until the resolved value's provenance no longer crosses such a boundary.

#### Scenario: Rooted context follows nested function provenance
- **WHEN** a value returned from `f(g(x))` still preserves provenance across both non-builtin function DAGs
- **THEN** `context(root=False)` returns the DAG for `f(...)`
- **AND** `context(root=True)` continues through `g(...)` until provenance no longer crosses another non-builtin import/function boundary

#### Scenario: Rooted context stops when outer DAG creates a fresh terminal value
- **WHEN** an outer non-builtin function DAG materializes a fresh value from an inner result and no longer preserves provenance across the inner boundary
- **THEN** both `context(root=False)` and `context(root=True)` return the outer DAG
