# Python authoring reference

Import DaggerML with its conventional namespace:

```python
import daggerml as dml
```

- `dml.new(name="", message="", dml=None) -> Dag`: create an open DAG runtime.
- `dml.load(name, dml=None) -> Dag`: load a committed named DAG from `HEAD`.
- `dml.temporary(**kw)`: context manager for an initialized temporary project.
- `Dag.put(value, name=None)`: stage a value and return a node.
- `Dag.call(fn, *args, name=None, timeout=-1)`: call a runnable and return its result node.
- `Dag.require(dag_name, node_name=None, name=None)`: import a prior committed node.
- `Dag.commit(value)`: finalize the DAG and record its result.
- `Node.value()`: materialize a value; `Node.context(root=True)`: trace provenance.

`dml.Dml(project_home=".")` opens an existing project. `dml.Dml.init(...)` initializes one programmatically, but researcher workflows should use `dml init` instead. `dml.Dml` also exposes low-level history, runtime, and administration namespaces; prefer the CLI for those operations.

For the higher-level `daggerml.contrib.api` helpers, including `funkify`, `ref`, `load`, `dagclass`, and `run`, see [author a DAG](../guides/author-a-dag.md).
