## ADDED Requirements

### Requirement: Public node wrappers SHALL expose provenance traversal through `context(root=...)`
The public `daggerml.api` node-wrapper surface SHALL expose `context(root: bool = True)` as the provenance-oriented way to resolve the DAG behind an imported, function-produced, builtin-derived, or projected value.

#### Scenario: Public node wrapper resolves nearest context
- **WHEN** a caller invokes `node.context(root=False)` on a public `Node`
- **THEN** the wrapper uses the shared API/runtime inspection surfaces to resolve the nearest non-builtin import/function DAG context for that value

#### Scenario: Public node wrapper resolves rooted context
- **WHEN** a caller invokes `node.context(root=True)` on a public `Node`
- **THEN** the wrapper recursively follows provenance until it no longer crosses a non-builtin import/function boundary and returns the resulting DAG

### Requirement: Public committed collection reads SHALL expose `Projection` wrappers for interrogation
The public `daggerml.api` collection-wrapper surface SHALL allow committed dict/list reads to return `Projection` wrappers for ex-post interrogation without mutating repository state.

#### Scenario: Committed collection read returns projection wrapper
- **WHEN** a caller reads a projected subvalue from a committed collection-valued `Node`
- **THEN** the public API may return a `Projection` wrapper instead of a real `Node` when the selected subvalue has no standalone persisted node identity

#### Scenario: Projection remains outside mutation and execution entrypoints
- **WHEN** a caller receives a public `Projection` wrapper
- **THEN** that wrapper is limited to read-only interrogation helpers and is not accepted as a staging, mutation, or callable-runtime input
