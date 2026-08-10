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
- `Node.value()`: materialize a value; `Node.context(root=True)`: trace provenance.

`Dag(tags=None)` accepts an optional list of tags for a named DAG. On a successful `commit`, DaggerML adds each tag to the named tree entry in the provided order. Tag mutations occur after the DAG commit and are not atomic with it: a tag-mutation error propagates while the DAG commit (and any earlier tag mutations) remains published.

Frozen DAGs remain uncommitted: named-node lookup, `keys()`, `values()`, and `argv` inspect their partial DAG, while `result` remains unavailable. Mutation methods are not implicitly unfrozen; call `unfreeze()` on the original wrapper before authoring further changes. To resume from a frozen runtime in another process, call `dml.resume(frozen, name=..., message=..., tags=...)`; all three metadata arguments are required because freezing does not preserve them (`tags=None` is an explicit no-tags choice).

`dml.Dml(project_home=".")` opens an existing project. `dml.Dml.init(...)` initializes one programmatically, but researcher workflows should use `dml init` instead. `dml.Dml` also exposes low-level history, runtime, and administration namespaces; prefer the CLI for those operations.

For the higher-level `daggerml.contrib.api` helpers, including `funkify`, `ref`, `load`, `dagclass`, and `run`, see [author a DAG](../guides/author-a-dag.md).
