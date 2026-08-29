## Why

Committed collection traversal produces `Projection` values that can be inspected but cannot currently be reused as inputs to new DAG work. Projections from the same `Dml` instance already contain the persisted base node and access path needed to reconstruct the selected value, so the codec system should materialize that recipe without adding usage-specific handling to staging or call entrypoints.

## What Changes

- Add a built-in `Projection` codec to recursive DAG value normalization.
- Materialize a projection by inserting an `ImportNode` for its committed base node and then inserting one builtin `get` access node for each projection path step.
- Support projections from the target DAG's `Dml` instance wherever codec normalization is applied, including direct, nested, and function-argument values.
- Preserve read-only traversal of the source committed DAG; materialization writes only to the active target DAG.
- Document projection reuse and its import-plus-access graph behavior in the Python authoring and codec documentation.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `node-provenance-projections`: Allow read-only projections to be materialized as runtime inputs through codec normalization while retaining no independent ref identity.
- `unified-dml-surface`: Accept projections at public codec-driven staging and execution entrypoints.
- `codec-normalization`: Add the built-in projection codec and define import-plus-access replay semantics.

## Impact

- `src/daggerml/api.py`: complete and register `ProjectionCodec` alongside the existing built-in codecs.
- `tests/api/contracts/` and `tests/api/integration/`: cover codec registration, import/access replay, recursive normalization, and live context-to-target reuse.
- `docs/use/reference/python-authoring.md`, `docs/use/concepts/dags-nodes-results.md`, `docs/use/concepts/artifacts-data-codecs.md`, and `docs/extend/reference/codec-contracts.md`: describe supported projection materialization and graph semantics.
- OpenSpec requirements governing projections, public staging, and codec normalization change from rejecting projection runtime inputs to accepting them through the codec system.
- No storage schema, runtime protocol, entry-point group, or external dependency changes are required.
