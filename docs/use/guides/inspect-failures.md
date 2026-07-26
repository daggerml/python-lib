# Inspect failures

Start with the repository and runtime state:

```bash
dml status
dml runtime list
dml runtime describe-graph INDEX_REF
dml show
```

For a committed failed DAG, inspect its result and provenance in Python:

```python
from daggerml import load

result = load("failed-analysis").result
print(result.value())
print(result.context())
```

CLI failures print `error: ...` to standard error. Common configuration failures are missing `remote.root` for remote-backed work and missing `remote.project` for project sync. See [errors](../reference/errors.md) before retrying or invalidating cache state.
