# Use a temporary project

`dml.temporary()` creates an initialized, isolated DaggerML project in a temporary directory. Its state is removed when the context exits, making it useful for experiments and tests.

```python
import daggerml as dml

with dml.temporary() as runtime:
    with dml.new("scratch", dml=runtime) as dag:
        result = dag.put("ephemeral")
        dag.commit(result)
```

Do not use temporary projects for results you need to retain or share. Create a normal project with `dml init` for durable work.
