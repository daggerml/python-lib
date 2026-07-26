# Runtimes

A runtime is the live, mutable state used while authoring a DAG or tracking work that has not yet become a committed DAG. A runtime holds the current graph and may have execution lineage. Finalizing it produces an immutable DAG.

The Python `Dag` wrapper manages its runtime for normal authoring. Use the CLI when you need to inspect or control open work:

```bash
dml runtime list
dml runtime describe INDEX_REF
dml runtime describe-graph INDEX_REF
dml runtime cancel INDEX_REF --mode full
```

`drive` cancellation additionally attempts to stop running tasks. See [inspect and cancel runtimes](../guides/runtime-inspection-cancellation.md).
