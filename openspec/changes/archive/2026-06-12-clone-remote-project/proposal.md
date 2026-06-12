## Why

The repository surface has `init`, `fetch`, and `pull`, but no single bootstrap workflow that behaves like `git clone`. Once unborn HEAD exists, DaggerML can support a proper `Dml.clone(...)` that initializes a local repo, records `remote.project`, fetches the selected remote ref, and leaves HEAD at the cloned ref.

## What Changes

- Add a `Dml.clone(...)` classmethod as the git-like bootstrap workflow for remote projects.
- Support bare project URIs by imputing `default.branch_name`.
- Support branch-qualified and tag-qualified project URIs by honoring the explicit selector.
- Persist `remote.project` and set local HEAD to the cloned ref state.

## Capabilities

### New Capabilities
- `clone-bootstrap-workflow`: Git-like remote bootstrap workflow for `Dml.clone(...)`.

### Modified Capabilities
- `unified-dml-surface`: the public `Dml` classmethod surface will gain `clone`.

## Impact

- Affected code: `src/daggerml/_core/dml.py`, `src/daggerml/_core/head.py`, remote sync orchestration, API/CLI constructor-generation tests.
- Affected behavior: repo bootstrap from remote refs, HEAD attachment/detachment after clone, default-branch imputation.
- Dependency: unborn-HEAD semantics must land first.
