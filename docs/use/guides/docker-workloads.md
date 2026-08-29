# Run Docker workloads

Use a Docker wrapper when the workload needs dependencies outside the authoring environment. Build or publish an image, then wrap an inner script funk with `uri="docker"`.

```python
from daggerml.contrib import api

@api.funkify(uri="docker", image=api.ref("image"))
@api.funkify
def analyze(dag, dataset):
    import pandas as pd
    return pd.read_parquet(dataset.value().uri).shape
```

The DAG must provide the `image` node. `daggerml.contrib.funks.docker_build` can build an image from an S3-backed context tarball. Without a registry repository, it stores the resulting Docker image as a gzip-compressed tar archive in S3; with a repository, it tags and pushes the image directly instead. Docker execution needs Docker, S3 access through `remote.root`, and an image containing the inner function's dependencies. See `examples/python/01-docker_dataset.py` for the complete pattern.
