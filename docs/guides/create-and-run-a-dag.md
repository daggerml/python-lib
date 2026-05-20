# Create and run a DAG

This is the smallest end-to-end DaggerML workflow that is easy to verify locally: initialize a repo, create a DAG in Python, commit the result, then inspect it from the CLI.

## 1. Initialize a repo

```bash
dml init --project-home ./demo-repo --user alice@example.com
```

That creates `.dml/` state under `./demo-repo` and leaves `HEAD` attached to `main`.

## 2. Create and commit a DAG in Python

```python
from daggerml import Dml, new

dml = Dml(project_home="./demo-repo", user="alice@example.com")

with new("numbers", message="create numbers dag", dml=dml) as dag:
    left = dag.put(2, name="left")
    right = dag.put(3, name="right")
    result = dag.put({"sum": left.value() + right.value()}, name="result")
    dag.commit(result)
```

Two practical details from the current API:

- Name the final node explicitly if you want a stable `result` entry in the committed DAG.
- Commit with `dag.commit(result)`. `Dag.result` is a read surface on committed DAGs, not a writable property.

## 3. Inspect the result from the CLI

```bash
dml --project-home ./demo-repo status
dml --project-home ./demo-repo dag get numbers
dml --project-home ./demo-repo show
```

Typical uses:

- `status` shows the attached branch, current commit, visible DAGs, and open indexes.
- `dag get numbers` shows the committed DAG summary, including names and result refs.
- `show` summarizes the current commit and the DAG-level change from its first parent.

## 4. Run a callable node when you need execution

The current Python API also supports `dag.call(...)` and `RunnableNode(...)` execution. A repo-backed example used in this codebase's tests looks like this:

```python
from pathlib import Path

from daggerml import Dml, Runnable, Uri, new

adapter = str(Path("tests/assets/internal_fn/python-fork-adapter.py").resolve())
fn = Runnable(target=Uri("./tests/assets/fns/sum.py"), adapter=adapter, kwargs={"x": 10})

dml = Dml(project_home="./demo-repo", user="alice@example.com")

with new("sum", message="run sum", dml=dml) as dag:
    total = dag.call(fn, 1, 2, 3, name="total")
    dag.commit(total)
```

Non-builtin execution may need remote runtime context, depending on the adapter you use.

## Related docs

- [DAGs and nodes](../concepts/dags-and-nodes.md)
- [Python API](../reference/python-api.md)
- [CLI](../reference/cli.md)
- [Errors](../reference/errors.md)
