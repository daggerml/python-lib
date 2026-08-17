# Python authoring reference

Import DaggerML with its conventional namespace:

```python
import daggerml as dml
```

- `dml.new(name="", message="", dml=None) -> Dag`: create an open DAG runtime.
- `dml.load(name, dml=None, *, revision="HEAD", remote=False, dep=None) -> Dag`: load a committed named DAG from local, fetched remote, or fetched dependency state.
- `dml.resume(frozen, *, name, message, tags, dml=None) -> Dag`: unfreeze a frozen runtime and reconstruct its authoring wrapper with explicit commit metadata.
- `dml.temporary(**kw)`: context manager for an initialized temporary project.
- `Dag.put(value, name=None)`: stage a value and return a node.
- `Dag.call(fn, *args, name=None, timeout=-1)`: call a runnable and return its result node.
- `Dag.require(dag_name, node_name=None, name=None)`: import a prior committed node.
- `Dag.freeze(message=None) -> Dag`: freeze an uncommitted runtime index for read-only inspection; records `dag: <name>` plus an optional newline-delimited annotation.
- `Dag.unfreeze() -> Dag`: restore a frozen uncommitted runtime index for authoring.
- `Dag.commit(value)`: finalize the DAG and record its result.
- `Dag.result`: return the result node of a successful committed DAG; raise its persisted `Error` when the committed DAG failed; reject uncommitted or terminal-less DAGs.
- `Node.value()`: materialize a value; `Node.context(root=True)`: trace provenance.

Committed collection indexing returns a read-only `Projection`. A projection can be inspected with `.value()` and `.context()` without changing its source DAG, or supplied anywhere the codec system accepts a value. When the projection and target DAG use the same `Dml` instance, encoding imports the projection's committed base node and records each string key, integer index, or slice as a builtin access node in the target DAG:

```python
val = dag.call(fn, *args)
ctx = val.context()
selected = dag.put(ctx.node_name["my_key"]["my_key1"])
```

This behavior is codec-driven, so the same projection can appear directly, inside a list or dictionary, or as a function argument. It does not copy the projected Python value into a new literal.

`Dag(tags=None)` accepts an optional list of tags for a named DAG. On a successful `commit`, DaggerML adds each tag to the named tree entry in the provided order. Tag mutations occur after the DAG commit and are not atomic with it: a tag-mutation error propagates while the DAG commit (and any earlier tag mutations) remains published.

Frozen DAGs remain uncommitted: named-node lookup, `keys()`, `values()`, and `argv` inspect their partial DAG, while `result` remains unavailable. Mutation methods are not implicitly unfrozen; call `unfreeze()` on the original wrapper before authoring further changes. To resume from a frozen runtime in another process, call `dml.resume(frozen, name=..., message=..., tags=...)`; all three metadata arguments are required because freezing does not preserve them (`tags=None` is an explicit no-tags choice).

`dml.Dml(project_home=".")` opens an existing project. `dml.Dml.init(...)` initializes one programmatically, but researcher workflows should use `dml init` instead. `dml.Dml` also exposes low-level history, runtime, and administration namespaces; prefer the CLI for those operations.

Low-level ref inspection returns names with exact commit tips:

```python
local_branches = session.branch.list()
remote_tags = session.tag.list(remote=True)
fetched_dependency_branches = session.branch.list(dep="models")
dependency_endpoint_branches = session.branch.list(remote=True, dep="models")
# [{"name": "main", "commit": Ref("commit:...")}]
```

For these list methods, `remote` selects endpoint state and `dep` selects the dependency, so both may be used together. Endpoint listing is read-only and does not fetch commits or update local tracking refs. `session.branch.get_upstream("feature")` returns `{"branch": "main"}` or `None`; tags have no upstream metadata. `session.runtime.read_execution_record(Ref("index:<execution-id>"))` returns unified lifecycle, adapter, lock, ref, lineage, cancelation, and invalidation state. Runtime creation, inspection, graph, and cancellation methods require `Ref` identities; they do not accept bare IDs or ref-shaped Python strings.

Cache control and garbage collection use direct low-level surfaces:

```python
cached_dag = session.cache.get(cache_key)
session.cache.invalidate(cache_key)
local_summary = session.gc()
remote_summary = session.gc(remote=True)
```

Cache and remote GC require `remote.root`; local GC does not. Remote GC never targets import-only dependencies. The removed `session.admin.remote` and `session.admin.gc` paths have no compatibility aliases.

For the higher-level `daggerml.contrib.api` helpers, including `funkify`, `ref`, `load`, `dagclass`, and `run`, see [author a DAG](../guides/author-a-dag.md).
