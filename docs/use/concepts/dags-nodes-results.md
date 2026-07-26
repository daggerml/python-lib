# DAGs, nodes, and results

A DAG is the stored record of one computation. It contains nodes for values, imports, and function calls, plus a final result. A `Dag` is mutable while being authored; after `commit`, it is an immutable snapshot that can be loaded and inspected.

Use `dag.put(value, name=...)` to stage a value, `dag.call(funk, ...)` to record a function call, and `dag.commit(node)` to make a result durable. Named nodes are convenient labels, not copies of data.

```python
from daggerml import load, new

with new("summary") as dag:
    values = dag.put([2, 3, 5], name="values")
    dag.commit(values)

assert load("summary").result.value() == [2, 3, 5]
```

Nodes materialize with `.value()`. Committed nested lists and dictionaries may return read-only projections; those also support `.value()` and `.context()`.
