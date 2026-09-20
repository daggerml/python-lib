# DaggerML Core

`_core` owns the typed repository model, LMDB persistence, mutable DAG/runtime
construction, configuration, revisions, remote transport, execution state, and
cache coordination. `Dml` composes those services; `head.py`, `commit.py`,
`dag.py`, and `index.py` implement repository operations; `types.py`, `db.pyx`,
and `serde.py` persist typed objects; `remote.py` and `exec_state.py` coordinate
remote work.

This is an application boundary: other package namespaces import only deliberate
package-level exports. Normative storage, execution, remote, and lifecycle
contracts are owned by the relevant OpenSpec capabilities listed in
[`openspec/spec-overview.md`](../../../openspec/spec-overview.md).

## Execution records

Remote execution state is split by ownership. Immutable metadata records cache,
execution, and argv identity. Semantic state records lifecycle, result, lineage,
cancellation, and invalidation. Driver state records locking, opaque adapter
continuation, shared retry deadlines, and cleanup outcome. Cache pointers are
published only after these records exist and name the current execution attempt.
Legacy unified or partial split records are stale rather than migrated.

A terminal result is reusable when semantic state has a published result and no
cancellation or invalidation blocks it. Cleanup is coordinated independently:
pending, delayed, or failed cleanup does not invalidate a published result.
Cancellation conditionally removes the selected attempt's matching cache pointer
before adapter teardown, and `cancel-pending` prevents later result mutation.
Adapter scratch paths and caller edges remain separate from the execution record.
