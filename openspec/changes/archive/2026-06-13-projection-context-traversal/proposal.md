## Why

Users can materialize DAG results easily today, but provenance-oriented traversal is awkward or unavailable. The public API needs a concise way to move from a value or projected subvalue to the nearest or ultimate non-builtin function/import DAG that produced it, especially when inspecting committed DAG results.

## What Changes

- Rename `Node.load()` to `Node.context()` and make it the public provenance-entrypoint for DAG traversal.
- Add `root: bool = True` to `context()` so callers can choose between the nearest non-builtin function/import context and the recursively rooted provenance context.
- Introduce a read-only `Projection` API object for committed-DAG subvalue traversal when a projected path does not correspond to a persisted node ref.
- Allow committed collection reads such as `node["foo"]` and nested projections to return `Projection` objects with read-only interrogation helpers such as `.value()`, `.context()`, indexing, `type`, `keys()`, and `len()`.
- Preserve builtin execution DAGs as internal traversal details rather than the main public provenance surface.

## Capabilities

### New Capabilities
- `node-provenance-projections`: Read-only projected subvalue traversal and provenance/context lookup across committed DAG results.

### Modified Capabilities
- `unified-dml-surface`: Public `Dag` and `Node` wrapper semantics change from `load()`-based node-context lookup to `context(root=...)` traversal and expose projection-based committed-result reads.

## Impact

- Affected code: `src/daggerml/api.py`, public API docs, and API contract tests for `Node`, `ListNode`, `DictNode`, and committed-DAG inspection behavior.
- Affected APIs: `Node.load()` rename/removal in favor of `Node.context()`, new `Projection` read-only surface, and committed collection traversal semantics.
- No storage or runtime protocol changes are expected; the change should remain implementable within the API layer by combining existing DAG/node inspection data with read-only projection logic.
