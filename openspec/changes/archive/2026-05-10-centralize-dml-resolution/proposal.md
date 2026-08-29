## Why

Resolution behavior for commits, DAGs, and nodes is currently split between `dml.py` and `dml_resolution.py`, which makes ambiguous selector handling harder to reason about and easier to drift out of sync. Centralizing all fuzzy resolution logic in one module will make the DML surface more predictable and give future callers a single contract for converting user selectors into canonical refs.

## What Changes

- Move all selector-resolution logic for commits, DAGs, and nodes into `src/daggerml/_internal/dml_resolution.py`.
- Define node resolution so it accepts either a direct node ref, a node-id style selector such as `node-literal:abc123`, or a node name plus optional dag selector.
- Require a dag selector only when named node resolution is ambiguous; direct node refs and node-id selectors resolve without DAG context.
- Standardize commit, DAG, and node resolution helpers to always return `Ref` instances for resolved objects.
- Remove remaining resolution logic from `src/daggerml/_internal/dml.py` so it delegates entirely to `dml_resolution.py`.

## Capabilities

### New Capabilities
- `dml-resolution`: Centralized DML selector resolution for commits, DAGs, and nodes with canonical `Ref` return values.

### Modified Capabilities
- None.

## Impact

- Affected code: `src/daggerml/_internal/dml.py`, `src/daggerml/_internal/dml_resolution.py`, and any callers/tests that depend on selector-resolution behavior.
- API impact: DML-facing selector behavior becomes more explicit around ambiguous node lookups and canonical ref returns.
- System impact: Resolution rules move closer to a single internal boundary, reducing duplicate parsing and validation paths.
