## 1. Type Model

- [x] 1.1 Refactor `Index` to inherit commit fields while retaining its mutable DAG state.
- [x] 1.2 Update serialization and validation logic for the new `Index` shape.

## 2. Runtime Integration

- [x] 2.1 Update `IndexOps.create(...)` to construct indexes from commit-shaped state.
- [x] 2.2 Keep runtime commit/finalization behavior unchanged and verify existing flows still pass.

## 3. Tests

- [x] 3.1 Add or update contract tests for `Index` model shape and validation.
- [x] 3.2 Run the runtime/history tests that exercise index creation and commit finalization.
