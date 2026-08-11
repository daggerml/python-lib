# DAG Storage and Types

`_core/types.py` defines the persistent model and namespace registry. Every
stored object is addressed by a `Ref` in `namespace:id` form. The namespace
selects its deserializing class and is validated whenever a typed field expects
a particular object family.

## Object graph

- `Datum` subclasses store scalar values, collections of datum refs, URIs, and
  persisted runnable specifications.
- `LiteralNode`, `ArgvNode`, `KwargvNode`, `ImportNode`, and `FnNode` connect
  values and computation within a DAG.
- `Dag` contains nodes, names, and exactly one terminal result or error when
  finished.
- `Tree` maps names to DAG refs and can attach opaque string-tag lists to those
  named entries; `Commit` snapshots a tree and history parents.
- `Error` is persisted data, so failed work remains inspectable in the graph.

`DmlDB` provides typed transactions over the Cython extension in `db.pyx`,
which wraps the native LMDB implementation under `c/`. Database objects live
in `.dml/db`; the filesystem stores lightweight control pointers such as
`.dml/HEAD`, local branch refs, mutable runtime refs, remote-tracking refs, and
`.dml/config.toml`.

Object identity and reachability depend on the explicit ref graph. Local
garbage collection starts from live pointers and removes unreachable objects;
changes to object shape, namespace registration, or references must preserve
that invariant. `Dml.gc()` selects this local path, while
`Dml.gc(remote=True)` selects the separate configured-remote maintenance path.

Tree tags are classification metadata rather than a new object type. A tag such
as `research.v0` has no repository-defined meaning; users or external tools can
interpret it as a schema convention. Tags are required in persisted tree data,
so repositories written before this field was added are not compatible with the
current v0 storage format.
