# Inspect a repository

Use the CLI when you want a quick JSON view of repo state, and use the Python API when you want the same information inside any Python environment, such as a script, a notebook, or a REPL.

## Check the current checkout

```bash
dml --project-home ./demo-repo status
```

Use `status` for the full picture.

Python equivalent:

```python
from daggerml import Dml

dml = Dml(project_home="./demo-repo")

print(dml.status())
```

## Look at commits and changes

```bash
dml --project-home ./demo-repo log --limit 5
dml --project-home ./demo-repo show
dml --project-home ./demo-repo diff
dml --project-home ./demo-repo diff --revision HEAD --relative-to HEAD~1
```

Notes for the generated CLI:

- `log` takes `--limit` because `limit` is an optional method parameter.
- `log` and `show` include the visible DAG map under `commit["dags"]`.
- `diff` defaults to `HEAD` and accepts `--relative-to` when you want an explicit base revision.

Python equivalent:

```python
from daggerml import Dml

dml = Dml(project_home="./demo-repo")

print(dml.log(limit=5))
print(dml.show())
print(dml.diff())
print(dml.diff("HEAD", "HEAD~1"))
```

## Inspect older revisions

Most repo and DAG inspection surfaces take revision selectors such as `HEAD`, `HEAD~1`, `main`, `@release`, or `dml://alice/demo#main`.

```bash
dml --project-home ./demo-repo show --revision HEAD~1
dml --project-home ./demo-repo show --revision @release
dml --project-home ./demo-repo show --revision dml://alice/demo#main
dml --project-home ./demo-repo log --limit 1
```

## Related docs

- [Commits and history](../concepts/commits-and-history.md)
- [CLI](../reference/cli.md)
- [Python API](../reference/python-api.md)
