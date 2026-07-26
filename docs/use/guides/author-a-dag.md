# Author a DAG

Initialize the project first with `dml init`. In an authoring script, create a DAG, stage values, optionally import a prior result, and commit the final node.

```python
from daggerml import new

with new("analysis", message="summarize inputs") as dag:
    raw = dag.put([1, 2, 3], name="raw")
    summary = dag.put({"count": len(raw), "values": raw}, name="summary")
    dag.commit(summary)
```

Use `dag.require("other-dag")` to import another committed DAG's result, or name a node explicitly. Load committed work with `load("analysis")`; committed DAGs are read-only.
