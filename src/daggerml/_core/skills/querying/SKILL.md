---
name: daggerml-querying
description: Extract data, traverse DAGs and provenance, and capture persisted errors.
---

# DaggerML Data Querying

Start from a project initialized with `dml init`. Query through Python when
selecting data or following provenance; use `dml` CLI inspection when you only
have refs. Do not modify managed `.dml/` files to read data.

## Load A DAG

Use `dml show` to discover committed DAG names and `dml.load(name,
revision="HEAD")` to load one. A loaded DAG is complete and immutable.
`dag.result` is its terminal node. A node merely named `"result"` is unrelated
and is accessed as `dag["result"]`.

Use `dml show` to obtain a DAG name or ref at the desired revision before
loading. Choose `.result` if the committed terminal output is what you want;
choose `dag["name"]` when the author explicitly retained an intermediate. The
two can point to different nodes. A missing name is not the same as a failed
named call: retain any error rather than silently substituting a fallback.

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

A projection has a `base` node and a `path`; it has no independent node `ref`.
On an open DAG, indexing instead records a new builtin access node in that
mutable graph. Do not assume an indexed value is always a new node.

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

In the example, `terminal` is the committed DAG's chosen output, while
`predictions` is a named node. Materialize `first_score`, not the whole
collection, if only that selection is needed. `context(root=False)` is useful
when looking for the function call that directly produced a value.

## Traverse Provenance

`node.context(root=False)` returns the nearest non-builtin function or import DAG
that produced the value. `node.context()` follows those boundaries to rooted
provenance. Builtin collection construction and access are transparent. The
returned object is another queryable `Dag`; inspect its names, `argv`, terminal
result, and errors.

For a result of a nested funk, compare `node.context(root=False)` with
`node.context()` before attributing a value to the outer authoring DAG. The
nearest context can be the inner execution DAG; the rooted context follows
function/import boundaries further. Inspect `context.keys()` and `context.argv`
to connect a value to the inputs and intermediate names actually recorded
there. Collection access alone is transparent to this traversal.

## Capture Persisted Errors

Failed work is durable data, not a missing node. Accessing a failed named
function node raises `daggerml.api.NodeError`. Retain its `origin`, `type`,
`message`, `stack`, and `node_ref`; `error.context()` returns the failed function
DAG. Accessing a failed terminal `dag.result` raises the persisted `Error`
directly.

The two failure paths differ: `dag["failed-call"]` raises `NodeError` with
`origin`, `type`, `message`, `stack`, and `node_ref`; `dag.result` of a failed
execution raises the persisted `Error`. Do not treat either as an absent value
or replace it with `None`. Retain the exact ref and failure context when
reporting an error so the producing execution can be inspected later.

When only refs are available, use a `Dml` session's `dag.describe`,
`dag.describe_node`, `dag.get_node`, and `dag.get_error` methods to follow the
node-to-function-DAG-to-error chain without discarding exact refs.

For CLI-only investigation, start with the committed ref and follow names to
node refs, then inspect the producing graph and error ref:

```bash
dml show
dml dag describe 'dag:YOUR_DAG_REF'
dml dag get-node-by-name 'dag:YOUR_DAG_REF' predictions
dml dag describe-node 'node:YOUR_NODE_REF'
# If the DAG description contains an error ref:
dml dag get-error 'error:YOUR_ERROR_REF'
```

Replace the placeholders with complete refs returned by the preceding
commands; do not strip their namespace prefixes. `describe-node` identifies
the producing DAG, whose description exposes result, names, and any error ref.

## Query A Partial DAG

An active runtime is mutable; a frozen runtime exposes its partial DAG as
read-only. Named nodes, `keys()`, `values()`, and `argv` remain queryable, but
`result` is unavailable until the DAG is committed.

When a runtime is active, querying a name does not freeze or commit it; other
work can still change the graph. A frozen runtime exposes a stable partial view
but is not a committed result. Select a named node or inspect `argv` when
working with either partial state, and only use `.result` after commit.
