# Manage artifacts

Use `Uri` values to record external artifacts without embedding large bytes in a DAG. With an S3 remote configured, `S3Store()` uses its data prefix.

```python
import daggerml as dml
from daggerml.contrib.s3 import S3Store

store = S3Store()
artifact = store.put(data=b"measurements", suffix=".txt")
with dml.new("inputs") as dag:
    result = dag.put(artifact, name="source")
    dag.commit(result)
```

Later, load the DAG and call `store.get(dml.load("inputs").result.value())`. `put` is content-addressed; `put_js`, `get_js`, `tar`, and `untar` support JSON and directory artifacts.
