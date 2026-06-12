# Storage Internals

DaggerML stores its real object graph in LMDB, but the repo is not LMDB alone. The `.dml/` directory mixes database state with a small filesystem-level pointer layer.

## Two kinds of state

### 1. Typed objects in LMDB

The database lives at `.dml/db`. `src/daggerml/_core/db.pyx` wraps the lower-level C implementation, and `src/daggerml/_core/types.py` exposes the typed `DmlDB` facade used by the rest of the runtime:

- transactions,
- namespace-aware put/get/delete/exists operations,
- iteration by namespace,
- orphan listing for GC,
- raw dump and load support for manifest-based transfer.

Objects are stored by namespace and object id. The Python side treats every stored object as a `Ref("namespace:id")`, then decodes the payload through the namespace registry in `types.py`.

### 2. Filesystem pointers in `.dml/`

Some state is easier to manage as small files than as database objects:

- `.dml/HEAD`: attached or detached head state,
- `.dml/refs/local/heads/*`: branch pointers,
- `.dml/refs/local/indexes/*`: mutable workspace pointers,
- `.dml/refs/remote/*`: tracking refs for fetched remote branches and tags,
- `.dml/config.toml`: project configuration.

That split gives DaggerML a Git-like control plane without forcing branch and checkout logic into the object store itself.

## Namespace-based identity

The object model depends on namespace prefixes being meaningful, not decorative. A ref like `dag:...` is different from `node-fn:...` or `datum-dict:...`, and code validates those expectations constantly.

This shows up in several places:

- `TxnContext.get()` uses `ref.ns()` to choose the Python class for deserialization.
- `require_ref()` in `types.py` checks namespace hierarchies such as `node` or `datum`.
- `TxnContext.put()` validates objects before writing and rejects unknown namespaces.

## Object graph shape

Committed state is built from a small set of object families:

- `Commit` points at a `Tree` and optional focal DAG.
- `Tree` maps names to DAG refs.
- `Dag` holds node refs, names, an optional result, an optional error, and optional argv state.
- `Node` objects point to datums directly or indirectly.
- `Datum` objects hold scalar values, collections of datum refs, URIs, or runnable specs.

Because all references are explicit, reachability is also explicit. Local GC starts from pointer roots discovered by `Head` and asks the DB layer which objects are no longer reachable.

## Transactions and retries

The DB layer is transactional, and the Python code leans on that heavily. Write operations are expected to either produce a whole coherent object graph or fail cleanly.

Most higher-level code goes through `DmlDB.tx()`, which yields a typed `TxnWithValid` wrapper over the raw LMDB transaction. That keeps object validation and namespace-aware reads and writes in one place instead of spreading raw DB access across the runtime.

## Local manifests

Remote publication and cross-process execution use a `local-manifest` shape produced from local storage. In practice this is a transport bundle containing:

- the root namespace and id,
- a closure of raw object dumps grouped by namespace,
- direct child DAG ids when the graph crosses a DAG boundary.

The important detail is that local manifests do not inline every child DAG recursively into one huge blob. They stop at child DAG refs and let the remote layer publish or resolve those DAGs separately. That keeps DAG boundaries visible in transport as well as in local storage.

## Why this design matters

The storage model is simple enough to reason about directly:

- the database stores immutable typed objects,
- refs and namespaces define identity,
- filesystem pointers choose which commits and indexes are live,
- manifests move closures across process and remote boundaries.

That simplicity is what lets the higher layers compose history, execution, caching, and sync without inventing separate persistence rules for each feature.
