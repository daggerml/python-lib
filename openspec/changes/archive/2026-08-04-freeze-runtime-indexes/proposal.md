## Why

Interactive DAG authoring needs a durable point at which a user can inspect completed intermediate nodes before continuing to construct the DAG. Current runtime indexes are mutable until commit and have no durable frozen form for this handoff.

## What Changes

- Add a persisted `FrozenIndex` runtime object that retains an index's partial DAG and execution identity while carrying an optional user message.
- Add `dml.runtime.freeze()` and `dml.runtime.unfreeze()` as inverse transitions that preserve the index ID.
- Allow runtime listing and description to inspect both active and frozen indexes.
- Continue to use generated index refs as durable identities; do not add named-index pointers or a name-resolution API.
- Treat frozen indexes as live roots for cancellation and cache-invalidation lineage.

## Capabilities

### New Capabilities
- `runtime-index-freezing`: Freeze a user runtime for durable intermediate-DAG inspection and later unfreeze it without changing its identity.

### Modified Capabilities

None.

## Impact

- Affects persistent runtime types, index transitions, runtime inspection, cancellation/GC reachability, the Python runtime namespace, and generated CLI commands.
- Affects `src/daggerml/_core/types.py`, `_core/index.py`, `_core/dml.py`, public API wrappers, CLI generation, and runtime contract tests.
