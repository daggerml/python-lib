## Why

`Dml.init()` currently manufactures a synthetic empty commit so every attached branch immediately has a materialized ref. That hides the natural git-like unborn-branch state and forces first-commit workflows to carry boilerplate history instead of letting the first real commit become the branch tip.

## What Changes

- Remove the synthetic initial commit from repository initialization.
- Allow attached `HEAD` to point at a branch whose ref file does not exist yet.
- Teach merge and related revision-handling workflows to accept `None` as "valid selector, unresolved commit" where appropriate.
- Materialize the branch ref only when the first real commit lands.

## Capabilities

### New Capabilities

### Modified Capabilities
- `git-like-commit-ops`: merge, branch, status, and revision workflows must handle unborn attached HEAD.
- `headops-pointer-management`: HEAD resolution must distinguish unborn attached branches from missing detached refs.
- `remote-project-refs`: init must create an unborn attached branch instead of an initial empty commit.

## Impact

- Affected code: `src/daggerml/_core/head.py`, `src/daggerml/_core/dml.py`, `src/daggerml/_core/commit.py`, `src/daggerml/_core/revision.py`, runtime/index tests.
- Affected behavior: init, first commit on a repo, branch creation on an unborn repo, revision resolution, merge on unborn HEAD.
- Follow-on dependency: `clone` will build on these semantics.
