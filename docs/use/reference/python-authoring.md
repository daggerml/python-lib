# Python authoring reference

Import authoring helpers from `daggerml`:

```python
from daggerml import Dml, Error, Ref, Runnable, Uri, load, new, temporary
```

- `new(name="", message="", dml=None) -> Dag`: create an open DAG runtime.
- `load(name, dml=None) -> Dag`: load a committed named DAG from `HEAD`.
- `temporary(**kw)`: context manager for an initialized temporary project.
- `Dag.put(value, name=None)`: stage a value and return a node.
- `Dag.call(fn, *args, name=None, timeout=-1)`: call a runnable and return its result node.
- `Dag.require(dag_name, node_name=None, name=None)`: import a prior committed node.
- `Dag.commit(value)`: finalize the DAG and record its result.
- `Node.value()`: materialize a value; `Node.context(root=True)`: trace provenance.

`Dml(project_home=".")` opens an existing project. `Dml.init(...)` initializes one programmatically, but researcher workflows should use `dml init` instead. `Dml` also exposes low-level history, runtime, and administration namespaces; prefer the CLI for those operations.
