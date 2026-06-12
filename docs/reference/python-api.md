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
- `db_path: str | None = None`
- `db_map_size_headroom: int | None = None`
- `db_map_size_max: int | None = None`
- `default_branch_name: str | None = None`
- `remote_project: str | None = None`
- `remote_root: str | None = None`
- `remote_prune_age_seconds: int | None = None`
- `remote_fetch_workers: int | None = None`
- `user: str | None = None`
- `config_home: str | None = None`

Top-level methods:

- `Dml.init(...)`: initialize a repository under `project_home`.
- `Dml.clone(project_uri, project_home=".", ...)`: initialize a repository, persist the branchless `remote.project`, fetch the selected remote ref, and leave `HEAD` at the cloned branch or tag state.
- `Dml.from_config_vars(...)`: construct a runtime from a flattened canonical config-var dictionary.
- `status()`: current HEAD, local branches, open runtime indexes, and same-name tracking counts when available.
- `log(revision="HEAD", limit=10)`: commit summaries, including each commit's visible `dags` map.
- `show(revision="HEAD")`: one commit summary plus DAG-level change information.
- `diff(revision="HEAD", relative_to=None)`
- `checkout(revision)`
- `fetch(project_uri)`
- `pull(ff_only=True)`
- `push(revision="HEAD", *, delete=False)`
- `merge(revision, ff_only=True)`
- `revert(revision, message=None)`

Namespaces exposed as properties:

- `dml.config`: `get`, `set`, `show`
- `dml.branch`: `list`, `create`, `move`, `rename`, `delete`
- `dml.tag`: `list`, `create`, `delete`
- `dml.runtime`: create, inspect, mutate, commit, list, and cancel runtime indexes
- `dml.dag`: inspect nodes, copy a DAG from history, and delete a DAG
- `dml.admin`: remote and GC operations

Representative repository examples:

```python
from daggerml import Dml

dml = Dml(project_home="./demo-repo")

remote = Dml(
    project_home="./demo-repo",
    remote_root="s3://my-bucket/demo",
    remote_fetch_workers=8,
)

canonical = Dml.from_config_vars(
    {
        "project_home": "./demo-repo",
        "remote.root": "s3://my-bucket/demo",
        "remote.fetch_workers": 8,
    }
)

print(dml.status())
print(dml.show("@release"))
clone_status = Dml.clone("dml://alice/demo#main", "./clone", remote_root="s3://my-bucket/demo")
dml = Dml(project_home="./clone")
dml.branch.create("feature", "HEAD~1")
dml.tag.create("v1")
dml.fetch("dml://alice/demo#main")
dml.push("@v1")
dml.push("#feature", delete=True)
```

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
- `temporary(**kw)` yields a temporary `Dml` initialized in a temporary directory with an unborn attached HEAD

`load()` raises `DmlRepoError(f"DAG not found: {name}")` when the name is missing.

## Working with DAGs

`new()` returns a mutable `Dag` wrapper backed by a runtime index.

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

## Related pages

- [CLI](cli.md)
- [Configuration](configuration.md)
- [Errors](errors.md)
