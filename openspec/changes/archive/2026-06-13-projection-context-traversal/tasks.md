## 1. Public API surface

- [x] 1.1 Replace the public `Node.load()` provenance entrypoint with `Node.context(root: bool = True)` and update the relevant docstrings and examples in `src/daggerml/api.py`.
- [x] 1.2 Define the read-only `Projection` wrapper in `src/daggerml/api.py` with the supported interrogation surface only: `.value()`, `.context()`, nested indexing, `type`, `keys()`, iteration, and length.

## 2. Provenance and projection behavior

- [x] 2.1 Implement committed dict/list traversal so projected reads can return `Projection` objects without staging runtime builtins.
- [x] 2.2 Implement nearest-context provenance resolution for real `Node` and `Projection` values by backtracking through builtin collection construction and builtin selection until the first non-builtin import/function boundary.
- [x] 2.3 Implement rooted-context recursion for real `Node` and `Projection` values so traversal continues until provenance no longer crosses a non-builtin import/function boundary.
- [x] 2.4 Keep builtin DAG contexts off the main public traversal surface; require lower-level `Dml` inspection for callers that need builtin-level details.

## 3. Tests and docs

- [x] 3.1 Update API contract tests for renamed `context()` behavior and add coverage for nearest vs rooted provenance traversal on imported, function-produced, and builtin-derived values.
- [x] 3.2 Add committed-DAG projection tests covering dict/list projection, nested projection chaining, `.value()`, `.context()`, and rejection of mutation/call semantics.
- [x] 3.3 Update the public Python API and DAG/node concept docs to describe `context(root=...)`, `Projection`, and committed-result interrogation patterns such as retrieving a nested function DAG's `uuid` value.
