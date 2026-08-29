# Inspect a completed DAG

Use `dml show` to identify the DAGs recorded by a revision. Load a named DAG in Python to inspect its stored graph:

```bash
dml show
```

```python
import daggerml as dml

dag = dml.load("analysis")
print(dag.ref)
print(dag.dml.dag.describe(dag.ref))
```

## Inspect named nodes

Name values and function calls while authoring so they remain easy to inspect after the DAG is committed. `dag.keys()` lists the names, and indexing a loaded DAG returns the corresponding node:

```python
import daggerml as dml

dag = dml.load("analysis")
print(dag.keys())

intermediate = dag["normalized_data"]
print(intermediate.type)
print(intermediate.value())
```

Named nodes are labels for stored values; they do not duplicate the underlying data.

## Traverse function sub-DAGs

The result of a function call has a provenance context. `context(root=False)` returns the nearest function or import DAG that produced a node, which lets you inspect that sub-DAG's named values and result:

```python
analysis = dml.load("analysis")
model_output = analysis["model_output"]

model_dag = model_output.context(root=False)
print(model_dag.keys())
print(model_dag.result.value())
```

Use `context()` with its default `root=True` to continue through function and import boundaries to the root provenance DAG.

## Inspect artifacts

DaggerML stores artifact URIs in the DAG rather than embedding large payloads. Read a named artifact through `S3Store`:

```python
from daggerml.contrib.s3 import S3Store

analysis = dml.load("analysis")
artifact_uri = analysis["source_data"].value()
payload = S3Store().get(artifact_uri)
```

`payload` is bytes. Decode it for text artifacts or pass it to the format-specific reader for the artifact. See [manage artifacts](artifacts.md) for artifact creation and supported helpers.

## Inspect rendered scripts

Name a staged funk when authoring the DAG so its persisted runnable can be inspected later. A script runnable stores the S3 URI of its rendered source in `script_uri`:

```python
import daggerml as dml
from daggerml.contrib.s3 import S3Store

runnable = dml.load("analysis")["normalize_text"].value()
script_uri = runnable.kwargs["script_uri"]
print(S3Store().get(script_uri).decode("utf-8"))
```

A wrapper runnable keeps its executor-specific fields and places the wrapped runnable in `sub`. For example, a Docker runnable has its image in `kwargs` and can wrap a script runnable:

```python
docker_runnable = dml.load("analysis")["normalize_in_docker"].value()
image = docker_runnable.kwargs["image"]
script_runnable = docker_runnable.sub
assert script_runnable is not None
script_uri = script_runnable.kwargs["script_uri"]
```

When a wrapper chain ends in a script runnable, `runnable.innermost()` is shorthand for retrieving that script runnable.
