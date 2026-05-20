## 1. Tighten core Dml contracts

- [x] 1.1 Narrow `src/daggerml/_internal/dml.py` method signatures so exact DB-object workflows require `Ref` and selector workflows keep string inputs.
- [x] 1.2 Remove ref-like string coercion from exact-input paths and add direct namespace validation for exact `Ref` inputs.
- [x] 1.3 Update DML payload shaping so DB-backed objects expose `Ref` as canonical identity and stop duplicating raw `id` fields.

## 2. Narrow shared resolution behavior

- [x] 2.1 Update `src/daggerml/_internal/dml_resolution.py` so it resolves lookup selectors only and no longer treats ref-like strings as exact refs.
- [x] 2.2 Keep DAG-name and node-name lookup flows working with revision and DAG context while rejecting ambiguous or unsupported selector forms.

## 3. Update callers and tests

- [x] 3.1 Update `src/daggerml/api.py` to pass `Ref` objects directly into strict `Dml` methods instead of `.to` strings.
- [x] 3.2 Update contract and unit tests to use `Ref` inputs for exact DB-object workflows and to assert failures for ref-like string inputs.
- [x] 3.3 Refresh any affected docs or generated help expectations so the new contract is documented consistently.
