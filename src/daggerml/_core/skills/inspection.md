---
name: daggerml-inspection
description: Inspect committed graphs, open runtimes, executions, errors, provenance, and cache state.
---

# DaggerML Inspection

A committed DAG is immutable and can be loaded and traversed. An active runtime
is mutable authoring state; a frozen runtime is the same partial graph made
read-only for inspection, not a completed execution. Remote executions have
separate lifecycle and lineage records. A failed function result is persisted
error context, not absent data.

```python
dag = dml.load("summary")
node = dag["summary"]
value = node.value()
context = node.context(root=True)
```

Start with a named node, then inspect its node description or provenance. For a
function-call result, traverse into the producing function DAG or imported DAG
rather than assuming the immediate value explains the result. Inspect persisted
errors with node context or `dml.dag.get_error`; retain the error's origin,
type, message, and stack when diagnosing failures.

Use `dml.runtime.describe` for active or frozen state,
`read_execution_record` and `describe_graph` for remote execution metadata and
lineage, and `cache.describe` to identify the exact execution behind a cache
key. To intentionally recompute, call `cache.invalidate` with the exact
`index:` or `frozenindex:` execution ref, never a cache key. For implementation
detail, inspect installed `daggerml._core.dag`, `daggerml._core.index`,
`daggerml._core.exec_state`, and `daggerml._core.dml`.
