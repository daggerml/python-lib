## 1. Extract Dml helper functions

- [x] 1.1 Inventory the current `Dml._...` helper methods in `src/daggerml/_internal/dml.py` and group them by responsibility (ops dispatch, payload shaping, revision binding, remote setup).
- [x] 1.2 Introduce equivalent module-level helper functions in `dml.py`, using explicit `dml` parameters wherever runtime state is required.
- [x] 1.3 Rewrite `Dml` public repository/bootstrap methods to use the new module-level helpers and remove the replaced private helper methods from `Dml`.

## 2. Simplify namespace implementations

- [x] 2.1 Rewrite runtime, DAG, admin, and config namespace methods to call module-level helpers instead of `self._dml._...` helper methods.
- [x] 2.2 Remove `_DagNamespace._stringify_node_selector` and replace it with a module-level utility.
- [x] 2.3 Confirm namespace instances retain only `._dml` as private state and do not introduce new private helper attrs or methods.

## 3. Preserve behavior and structural contracts

- [x] 3.1 Update tests that inspect `Dml` or namespace structure to assert the allowed remaining private attrs and the absence of removed private helper methods.
- [x] 3.2 Run targeted contract and integration tests covering `Dml` repository methods, namespace workflows, and any call paths sensitive to ops lifecycle behavior.
- [x] 3.3 Resolve any regressions without expanding the refactor scope beyond the documented boundary cleanup.
