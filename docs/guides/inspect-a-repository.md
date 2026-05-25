# Inspect a repository

Use the CLI when you want a quick JSON view of repo state, and use the Python API when you want the same information inside any Python environment, such as a script, a notebook, or a REPL.

## Check the current checkout

```bash
dml --project-home ./demo-repo status
dml --project-home ./demo-repo branch
```

Use `status` for the full picture and `branch` when you only care about branch names and the current attached head. Pass one branch name to create it from the current HEAD commit.

Python equivalent:

```python
from daggerml import Dml

dml = Dml(project_home="./demo-repo")

print(dml.status())
print(dml.branch())
print(dml.branch("feature"))
```

## Look at commits and changes

```bash
dml --project-home ./demo-repo log --limit 5
dml --project-home ./demo-repo show
dml --project-home ./demo-repo diff --left HEAD~1 --right HEAD
```

Notes for the generated CLI:

- `log` takes `--limit` because `limit` is an optional method parameter.
- `log` and `show` include the visible DAG map under `commit["dags"]`.
- `diff` uses `--left` and `--right` for the same reason.

Python equivalent:

```python
from daggerml import Dml

dml = Dml(project_home="./demo-repo")

print(dml.log(limit=5))
print(dml.show())
print(dml.diff("HEAD~1", "HEAD"))
```

## Inspect DAGs at a revision

```bash
dml --project-home ./demo-repo dag get numbers
```

Use `show` to discover visible DAG names, then `dag get` when you want one DAG summary.

Python equivalent:

```python
from daggerml import Dml

dml = Dml(project_home="./demo-repo")

print(dml.show())
print(dml.dag.get("numbers"))
```

## Inspect older revisions

Most repo and DAG inspection surfaces take revision selectors such as `HEAD`, `HEAD~1`, `main`, or `origin/main`.

```bash
dml --project-home ./demo-repo show --revision HEAD~1
dml --project-home ./demo-repo log --limit 1
```

## Related docs

- [Commits and history](../concepts/commits-and-history.md)
- [CLI](../reference/cli.md)
- [Python API](../reference/python-api.md)
