# Store and load external data

Use DaggerML to track references to large external artifacts, not to inline the bytes into repository state. The current repo support for this lives in `Uri` values and `daggerml.contrib.s3.S3Store`.

## 1. Create a store

If your repo already has `remote.root` configured, `S3Store()` uses that root automatically.

```python
from daggerml.contrib.s3 import S3Store

store = S3Store()
```

If you want to be explicit, construct the store from an S3 remote root:

```python
from daggerml.contrib.s3 import S3Store

store = S3Store.from_remote_root("s3://bucket/prefix")
```

## 2. Upload bytes and commit the resulting `Uri`

```python
from daggerml import Dml, new
from daggerml.contrib.s3 import S3Store

dml = Dml(project_home="./demo-repo", remote_root="s3://bucket/prefix", user="alice@example.com")
store = S3Store.from_remote_root("s3://bucket/prefix")

artifact_uri = store.put(data=b"hello world", suffix=".txt")

with new("artifacts", message="store external data", dml=dml) as dag:
    result = dag.put(artifact_uri, name="result")
    dag.commit(result)
```

The DAG now stores a `Uri`, while the payload bytes stay in S3.

## 3. Load the `Uri` later and read the payload

```python
from daggerml import Dml, load
from daggerml.contrib.s3 import S3Store

dml = Dml(project_home="./demo-repo", remote_root="s3://bucket/prefix")
store = S3Store.from_remote_root("s3://bucket/prefix")

artifact_uri = load("artifacts", dml=dml).result.value()
payload = store.get(artifact_uri)
```

## 4. Common follow-up operations

```python
listed = store.ls(recursive=True)
exists = store.exists(artifact_uri)
store.rm(artifact_uri)
```

`S3Store` also supports JSON helpers with `put_js()` and `get_js()`, plus tarball upload and extraction with `tar()` and `untar()`.

## Related docs

- [Storage](../concepts/storage.md)
- [Codecs and values](../concepts/codecs-and-values.md)
- [Python API](../reference/python-api.md)
- [Errors](../reference/errors.md)
