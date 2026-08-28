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
- `Dag` contains nodes, names, normalized opaque tags, and exactly one terminal
  result or error when finished.
- `Tree` maps names to DAG refs; `Commit` snapshots a tree and history parents.
- `Error` is persisted data, so failed work remains inspectable in the graph.

`DmlDB` provides typed transactions over the Cython extension in `db.pyx`,
which wraps the native LMDB implementation under `c/`. Database objects live
in `.dml/db`; the filesystem stores lightweight control pointers such as
`.dml/HEAD`, local branch refs, mutable runtime refs, remote-tracking refs, and
`.dml/config.json`.

`.dml/shallow.json` is exact version-0 local availability metadata containing a
sorted, unique list of exact commit refs intentionally absent behind
materialized history. It does not alter `Commit.parents` or any content-derived
identity. A materialized commit always retains a complete tree/DAG closure;
only a declared commit parent may be absent.

Object identity and reachability depend on the explicit ref graph. Local
garbage collection starts from live pointers and removes unreachable objects;
changes to object shape, namespace registration, or references must preserve
that invariant. `Dml.gc()` selects this local path, while
`Dml.gc(remote=True)` selects the separate configured-remote maintenance path.
Native local reachability accepts declared absent commits as terminal leaves
but still fails on missing roots, snapshots, or undeclared refs. Collection also
removes shallow entries no longer referenced by retained objects.

DAG tags are classification metadata rather than a new object type. A tag such
as `research.v0` has no repository-defined meaning; users or external tools can
interpret it as a schema convention. Tags are required in persisted DAG data and
are unique and lexicographically sorted, so this v0 format is incompatible with
repositories that use the former tree-owned tag representation.
