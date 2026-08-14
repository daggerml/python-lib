# DAGs, nodes, and results

A DAG is the stored record of one computation. It contains nodes for values, imports, and function calls, plus a final result. A `Dag` is mutable while being authored; after `commit`, it is an immutable snapshot that can be loaded and inspected.

Use `dag.put(value, name=...)` to stage a value, `dag.call(funk, ...)` to record a function call, and `dag.commit(node)` to make a result durable. Named nodes are convenient labels, not copies of data.

```python
import daggerml as dml

with dml.new("summary") as dag:
    values = dag.put([2, 3, 5], name="values")
    dag.commit(values)

assert dml.load("summary").result.value() == [2, 3, 5]
```

Nodes materialize with `.value()`. Committed nested lists and dictionaries may return read-only projections; those also support `.value()` and `.context()`. Creating and extending a projection only reads the committed source DAG.

A projection from the same `Dml` instance can become an input to new work. The normal codec path imports its persisted base node into the active DAG, then records one builtin `get` node for each projected key, index, or slice. Direct puts, nested collections, and function arguments all use this shared normalization behavior, preserving graph provenance instead of copying the materialized Python subvalue.

Failed function calls retain their named node and terminal error ref. High-level access to a failed node raises `NodeError`; use its `.context()` to inspect the failed function DAG, or use `dml.dag.get_node(node_ref)` and `dml.dag.get_error(error_ref)` for low-level error inspection.
