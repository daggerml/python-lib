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
