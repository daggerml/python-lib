---
name: daggerml-inspection
description: Inspect committed graphs, open runtimes, executions, errors, provenance, and cache state.
---

# DaggerML Inspection

First identify the state: a committed DAG is immutable and loadable; an active
runtime is mutable authoring state; a frozen runtime is its read-only partial
graph, not a completed execution. Remote executions have separate lifecycle
and lineage records. A failed function result is persisted error context, not
absent data.

```python
dag = dml.load("summary")
node = dag["summary"]
value = node.value()
context = node.context(root=True)
```

Start with a named node: materialize its value, then inspect its description or
provenance. For a function-call result, traverse into the producing function
DAG or imported DAG rather than assuming the immediate value explains it.
Inspect persisted errors with node context or `dml.dag.get_error`; retain the
origin, type, message, and stack when diagnosing failures.

Use `dml.runtime.describe` for active or frozen state. For remote work, read
the execution record and `describe_graph` for metadata and lineage. To diagnose
a cache result, call `cache.describe` and retain its exact execution ref. To
intentionally recompute, call `cache.invalidate` with that `index:` or
`frozenindex:` ref, never a cache key, then describe the cache again. For
implementation detail, inspect installed `daggerml._core.dag`,
`daggerml._core.index`, `daggerml._core.exec_state`, and
`daggerml._core.dml`.
