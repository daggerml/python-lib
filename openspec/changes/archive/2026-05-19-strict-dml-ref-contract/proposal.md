## Why

`Dml` currently mixes DB object identity across `Ref`, ref-like strings, and raw `id` fields. That makes method contracts hard to predict and pushes selector parsing into APIs that should only accept exact object handles.

## What Changes

- **BREAKING** Require `Ref` objects for all `Dml` inputs that represent DB objects.
- **BREAKING** Stop accepting ref-like strings such as `"dag:..."`, `"node:..."`, and `"commit:..."` where the method contract is for an exact DB object.
- Keep string inputs for non-DB selectors and labels such as revisions, DAG names, node names, branches, tags, remote URIs, and `index_id` values.
- Narrow `Dml` read and mutation surfaces so lookup-oriented methods are selector-based and dereference/mutation methods are ref-based.
- Remove duplicated raw DB `id` fields from `Dml` payloads and return `Ref` objects as the canonical DB identity.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `unified-dml-surface`: tighten the shared `Dml` contract so DB object inputs and outputs are ref-based, while non-DB selectors remain string-based.
- `dml-resolution`: limit the shared resolution layer to selector-to-ref lookup flows and remove ref-like string coercion from exact-input APIs.

## Impact

- Affected code: `src/daggerml/_internal/dml.py`, `src/daggerml/_internal/dml_resolution.py`, `src/daggerml/api.py`, and callers/tests that pass `.to` strings instead of `Ref` objects.
- Affected APIs: shared internal `Dml`, high-level Python wrappers that delegate through `Dml`, and any generated CLI/help metadata derived from signatures.
- Affected payloads: DAG, node, and commit payload shapes that currently duplicate `id` and `ref` for DB-backed objects.
