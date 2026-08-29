## 1. Refactor init layout bootstrap

- [x] 1.1 Update `DmlOps.init` in `src/daggerml/_internal/ops/__init__.py` to use `init_project_layout` for `.dml` directory, `.gitignore`, and local config bootstrap when initialization needs to create missing layout artifacts.
- [x] 1.2 Keep existing init contract intact by preserving argument handling, default URI derivation, validation errors, return payload keys, and recovery pull gating behavior.

## 2. Remove obsolete duplicated code

- [x] 2.1 Remove `DmlOps` private helpers that become unused after delegating bootstrap writes (for example inline config/gitignore-writing helpers), and update imports/usages accordingly.
- [x] 2.2 Ensure no remaining duplicated filesystem bootstrap logic exists in `DmlOps.init` that overlaps with `init_project_layout`.

## 3. Verify behavior with tests

- [x] 3.1 Update or add tests in init-focused internal suites to assert contract-preserving behavior after helper delegation, including recovery-mode behavior.
- [x] 3.2 Run targeted tests for init and project workflow paths to confirm refactor correctness and unchanged external semantics.
