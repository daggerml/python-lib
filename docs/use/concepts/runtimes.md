# Runtimes

A runtime is the live state used while authoring a DAG or tracking work that has not yet become a committed DAG. A runtime holds the current graph and may have execution lineage. Finalizing it produces an immutable DAG.

A user runtime can be frozen after recording an intermediate result. Freezing preserves its partial DAG and execution identity but does not create a terminal DAG result. Inspect its named nodes, then unfreeze it to continue authoring. The freeze message is a human-readable reason, not a durable runtime name.

The Python `Dag` wrapper manages its runtime for normal authoring. Use the CLI when you need to inspect or control open work:

```bash
dml runtime list
dml runtime describe INDEX_REF
dml runtime freeze INDEX_REF --message "Review implementation"
dml runtime unfreeze FROZEN_INDEX_REF
dml runtime describe-graph INDEX_REF
dml runtime cancel INDEX_REF --mode full
```

`drive` cancellation additionally attempts to stop running tasks. See [inspect and cancel runtimes](../guides/runtime-inspection-cancellation.md).
