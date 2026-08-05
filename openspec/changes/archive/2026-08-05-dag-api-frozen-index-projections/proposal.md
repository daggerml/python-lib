## Why

`Dml.runtime.freeze()` and `Dml.runtime.unfreeze()` already support user runtime indexes, but the high-level `api.Dag` wrapper has no corresponding lifecycle methods. A frozen index should remain inspectable through the normal DAG projection surface without being treated as a completed DAG or permitting mutation.

## What Changes

- Add `Dag.freeze(message: str | None = None)` and `Dag.unfreeze()` as API-only wrappers around the existing runtime operations. Each updates `Dag.token` to the returned active or frozen index reference. Freeze annotations are always `dag: <Dag.name>`, with a non-empty per-freeze message appended after a newline.
- Route high-level named-node reads for uncommitted indexes, including frozen indexes, through the partial DAG ref from `runtime.describe(index)["dag"]` and the same `dml.dag.describe(...)` projection path used by completed DAGs.
- Keep frozen indexes immutable. No API mutation method will implicitly thaw or refreeze them; existing runtime mutation errors remain authoritative.
- Document the lifecycle and read-only projection behavior in the Python authoring reference.

## Capabilities

### New Capabilities

- `dag-api-frozen-index-projections`: High-level Dag lifecycle helpers and read-only projections for frozen runtime indexes.

### Modified Capabilities

## Impact

- Affected code: `src/daggerml/api.py` and API contract tests only.
- Affected docs/specs: Python authoring reference and this OpenSpec capability.
- Explicitly excluded: all `src/daggerml/_core/**` changes, automatic thawing, and mutation support for frozen indexes.
