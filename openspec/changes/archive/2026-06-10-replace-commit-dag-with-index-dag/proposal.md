## Why

`Commit.dag` currently mixes immutable history with mutable workspace state, so commit objects and commit-oriented descriptions carry a "current DAG" concept that really belongs to the index. We need to move current-DAG ownership into `Index` so commit history only describes recorded tree state while index workflows remain the only place that tracks in-progress DAG mutation.

## What Changes

- **BREAKING**: Remove `Commit.dag` and replace it with `Index.dag` as the sole current-DAG pointer for mutable runtime work.
- Update index mutation and execution flows to read and write the current DAG through the index object instead of through the index head commit.
- Keep committed DAG publication rooted in the commit tree, so named DAGs still enter history through `tree.dags` rather than through a dedicated commit-level DAG field.
- Change runtime finalization so `IndexOps.commit()` returns `(dag_ref, commit_ref | None)`, where `commit_ref` is only created when `name is not None`.
- Keep unnamed finalized DAGs out of the commit tree and out of `HEAD`: when `name is None`, finalize and publish the DAG for execution/cache consumers but do not create or advance a commit.
- Remove `commit.dag` from commit description and commit-inspection outputs, and update any runtime/index descriptions that need to expose the current DAG to read it from the index instead.
- Rewrite affected tests so they assert the new ownership boundary and the updated inspection payloads.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `git-like-commit-ops`: remove commit-level current-DAG expectations so commit workflows and descriptions are defined in terms of commit trees and DAG-map deltas only, while unnamed runtime finalization produces a DAG without creating a history commit.
- `repo-inspection-cli`: remove `commit.dag` from commit-facing inspection payloads while keeping DAG-map inspection rooted in the selected revision's tree.

## Impact

- Affected code: `src/daggerml/_core/types.py`, `src/daggerml/_core/index.py`, `src/daggerml/_core/commit.py`, `src/daggerml/_core/dml.py`, and any commit/index serialization helpers.
- Affected tests: `_core` runtime/index tests, commit inspection tests, and remote roundtrip coverage that currently asserts `Commit.dag`.
- Affected behavior: commit objects and commit descriptions lose the dedicated `dag` field; index-owned current-DAG flows must continue to support import, builtin execution, adapter execution, commit finalization, and runtime describe/list surfaces. `dml.runtime.commit(...)` now returns a DAG ref in all cases and only advances history when a named DAG commit is created.
- Compatibility: this is intentionally breaking for internal code and any callers that read `commit.dag` from serialized payloads.
