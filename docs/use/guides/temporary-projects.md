# Use a temporary project

`temporary()` creates an initialized, isolated DaggerML project in a temporary directory. Its state is removed when the context exits, making it useful for experiments and tests.

```python
from daggerml import new, temporary

with temporary() as dml:
    with new("scratch", dml=dml) as dag:
        result = dag.put("ephemeral")
        dag.commit(result)
```

Do not use temporary projects for results you need to retain or share. Create a normal project with `dml init` for durable work.
