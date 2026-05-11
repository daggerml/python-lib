## 1. Resolution Helpers

- [x] 1.1 Expand `src/daggerml/_internal/dml_resolution.py` with shared helpers for canonical commit, DAG, and node resolution.
- [x] 1.2 Implement node selector handling for direct node refs, canonical node-id style selectors, and named node lookups with clear ambiguity errors.
- [x] 1.3 Ensure DAG and node resolution helpers return canonical `Ref` instances and reject incompatible selector combinations.

## 2. DML Integration

- [x] 2.1 Remove selector parsing and ambiguity logic from `src/daggerml/_internal/dml.py` and delegate to `dml_resolution.py`.
- [x] 2.2 Update any other internal DML call sites that depend on mixed resolution return shapes to use the new shared helper contract.

## 3. Verification

- [x] 3.1 Add or update tests covering revision, DAG, and node resolution with direct refs, raw ids, named selectors, and invalid inputs.
- [x] 3.2 Add or update tests covering ambiguous named node lookup and the requirement for explicit DAG disambiguation when needed.
- [x] 3.3 Run the relevant DML and selector-related test suite and fix any regressions.
