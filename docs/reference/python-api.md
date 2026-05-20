# Python API

The main Python entrypoint is the package root:

```python
from daggerml import Dml, Error, Ref, Runnable, Uri
from daggerml import clear_default_dml, get_default_dml, load, new, set_default_dml
from daggerml import status, temporary, use_default_dml
```

Those names come from `src/daggerml/__init__.py` and `src/daggerml/api.py`.

## Runtime and helper exports

### `Dml`

`Dml` is the session object for repository, history, runtime, DAG, and admin workflows.

Constructor arguments:

- `project_home: str | None = None`
- `remote_root: str | None = None`
- `user: str | None = None`
- `config_home: str | None = None`

Top-level methods:

- `Dml.init(...)`: initialize a repository under `project_home`.
- `status()`: current HEAD, branches, DAGs, and open runtime indexes.
- `branch(remote=False)`: list local branches or discovered remote-tracking branches.
- `log(revision="HEAD", limit=None)`: commit summaries, including each commit's visible `dags` map.
- `show(revision="HEAD")`: one commit summary plus DAG-level change information.
- `diff(left="HEAD~1", right="HEAD")`
- `checkout(revision)`
- `fetch(remote_or_uri, branch=None)`
- `pull(remote_or_uri, remote_branch=None, *, branch=None, user)`
- `push(tag=None, *, branch=None, create=False, force=False)`
- `merge(revision, *, branch=None, user)`
- `revert(revision, *, branch=None, user)`

Namespaces exposed as properties:

- `dml.config`: `get`, `set`, `show`
- `dml.runtime`: create, inspect, mutate, commit, list, delete, and cancel runtime indexes
- `dml.dag`: get, inspect nodes, copy a DAG from history, and delete a DAG
- `dml.admin`: cache, remote, and GC operations

### Default-runtime helpers

The convenience helpers in `daggerml.api` keep a process-global or context-local default `Dml` instance.

- `get_default_dml()` returns the active default and creates one if needed.
- `set_default_dml(dml)` installs a process default.
- `clear_default_dml()` removes the process default.
- `use_default_dml(dml)` temporarily overrides the default in the current context.
- `status()` returns both default-runtime metadata and `dml.status()`.

Resolution order:

1. the active `use_default_dml(...)` scoped override
2. the process default set through `set_default_dml(...)`
3. a lazily created implicit `Dml()` instance

That implicit instance is cached after first creation, so later top-level helpers reuse it instead of constructing a fresh runtime each time.

`status()` is designed to stay JSON-serializable. It reports the default-runtime source plus the active runtime's config and repository status instead of returning live Python objects.

### Repository helpers

- `new(name="", *, message="", argv_ptr=None, dml=None) -> Dag`
- `load(name: str, dml=None) -> Dag`
- `temporary(**kw)` yields a temporary `Dml` initialized in a temporary directory

`load()` looks up a named DAG through `dml.dag.get(name)` and raises `DmlRepoError(f"DAG not found: {name}")` when the name is missing.

## Working with DAGs

`new()` returns a mutable `Dag` wrapper backed by a runtime index.

```python
from daggerml import Dml, new

dml = Dml(project_home=".")

dag = new("demo", message="first dag", dml=dml)
answer = dag.put(42, name="answer")
dag.commit(answer)
```

Important `Dag` behavior:

- `dag["name"]` is the canonical named-node lookup.
- `dag.name` falls back to named-node lookup only when `name` is not already a real `Dag` attribute.
- `dag.result` is the committed DAG result property.
- `dag["result"]` looks up a node literally named `"result"`.
- `dag.put(value, name=None)` stages Python values through the codec system and returns a `Node` wrapper.
- `dag.call(fn, *args, name=None, sleep=None, timeout=-1, **kw)` stages a function call and returns the result node.
- `dag.commit(value)` writes the DAG result into repository history.
- If you use `Dag` as a context manager, uncaught exceptions are converted to `Error` values and committed.

Using `with dag:` is for error capture. Successful DAGs still need an explicit `dag.commit(...)` call.

`Dag.call()` stages plain Python values and existing node arguments into the current runtime index before execution. It raises `TimeoutError` if the call does not finish before `timeout`.

## Node wrappers

Every staged or committed node is wrapped as one of these classes:

- `ScalarNode`: scalar values, including `Uri`
- `RunnableNode`: callable `Runnable` values
- `ListNode`: list-like values
- `DictNode`: dict-like values

Common node methods:

- `node.value()`: materialize the concrete value
- `node.load()`: load the DAG that owns the node
- `node.argv`: access the node's argv list when present
- `node.type`: cached type label such as `list`, `dict`, or `runnable`

Collection helpers:

- `ListNode[i]`, `ListNode[start:stop]`
- `ListNode.append(item)` / `ListNode.conj(item)`
- `DictNode[key]`
- `DictNode.get(key, default=None)`
- `CollectionNode.contains(item)`

`RunnableNode(*args, **kw)` delegates to `dag.call(...)` and returns another node.

## Value types

The public value wrappers re-exported from the package are:

- `Ref`: persistent object reference
- `Uri`: URI-backed datum
- `Runnable`: callable datum stored in the graph
- `Error`: captured execution error with `message`, `origin`, `type`, and `stack`

## Example: create and read a DAG

```python
from daggerml import Dml, load, new

dml = Dml(project_home=".")

dag = new("numbers", message="store a list", dml=dml)
values = dag.put([1, 2, 3], name="values")
dag.commit(values)

saved = load("numbers", dml=dml)
print(saved["values"].value())
print(saved.result.value())
```

## Related pages

- [CLI](cli.md)
- [Configuration](configuration.md)
- [Errors](errors.md)
