---
name: daggerml-querying
description: Extract data, traverse DAGs and provenance, and capture persisted errors.
---

# DaggerML Data Querying

## Load A DAG

Use `dml show` to discover committed DAG names and `dml.load(name,
revision="HEAD")` to load one. A loaded DAG is complete and immutable.
`dag.result` is its terminal node. A node merely named `"result"` is unrelated
and is accessed as `dag["result"]`.

## Get Nodes And Values

Read named nodes with `dag.foo` or `dag["foo"]`. Use item syntax for names that
collide with `Dag` attributes or methods. `dag.keys()` lists names and
`dag.values()` returns their nodes.

Nodes remain graph objects until `.value()` materializes Python data. Materialize
only when concrete data is needed; nodes and projections can be traversed and
inspected without materializing their parent values.

## Access Collections

Collection-valued nodes support key, index, and slice access. A completed DAG
cannot record new access nodes, so `dag.foo["bar"]` returns a read-only
`Projection`, not a new node. It follows the same value semantics:
`dag.foo["bar"].value() == dag.foo.value()["bar"]`. Chain projections to select
only the required data. Projections also support `.context()`.

```python
import daggerml as dml
import daggerml.api as api

dag = dml.load("experiment")
terminal = dag.result
predictions = dag["predictions"]
first_score = predictions["rows"][0]["score"]

print(first_score.value())
print(first_score.context(root=False).keys())

try:
    dag["failed-call"]
except api.NodeError as error:
    print(error.origin, error.type, error.message, error.stack)
    failed_call = error.context()
```

## Traverse Provenance

`node.context(root=False)` returns the nearest non-builtin function or import DAG
that produced the value. `node.context()` follows those boundaries to rooted
provenance. Builtin collection construction and access are transparent. The
returned object is another queryable `Dag`; inspect its names, `argv`, terminal
result, and errors.

## Capture Persisted Errors

Failed work is durable data, not a missing node. Accessing a failed named
function node raises `daggerml.api.NodeError`. Retain its `origin`, `type`,
`message`, `stack`, and `node_ref`; `error.context()` returns the failed function
DAG. Accessing a failed terminal `dag.result` raises the persisted `Error`
directly.

When only refs are available, use a `Dml` session's `dag.describe`,
`dag.describe_node`, `dag.get_node`, and `dag.get_error` methods to follow the
node-to-function-DAG-to-error chain without discarding exact refs.

## Query A Partial DAG

An active runtime is mutable; a frozen runtime exposes its partial DAG as
read-only. Named nodes, `keys()`, `values()`, and `argv` remain queryable, but
`result` is unavailable until the DAG is committed.
