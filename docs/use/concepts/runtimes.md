# Runtimes

A runtime is the live state used while authoring a DAG or tracking work that has not yet become a committed DAG. A runtime holds the current graph and may have execution lineage. Finalizing it produces an immutable DAG.

A user runtime can be frozen after recording an intermediate result. Freezing preserves its partial DAG and execution identity but does not create a terminal DAG result. Inspect its named nodes, then unfreeze it to continue authoring. The freeze message is a human-readable reason, not a durable runtime name.

The Python `Dag` wrapper manages its runtime for normal authoring. Use the CLI when you need to inspect or control open work:

```bash
dml runtime list
dml runtime describe index:<execution-id>
dml runtime freeze index:<execution-id> --message "Review implementation"
dml runtime unfreeze frozenindex:<execution-id>
dml runtime describe-graph index:<execution-id>
dml runtime cancel index:<execution-id> --mode full
```

At the public `Dml.runtime` boundary, runtime and execution identities are `Ref` values. In Python, cancellation is `Dml.runtime.cancel(execution=Ref(...), mode=...)`; `execution` is the requested execution identity. Internal execution coordination still stores and exchanges the ID portion as a string. `full` cancellation first selects the complete unreferenced execution set as `cancel-pending`, then asks each selected adapter to stop and records `canceled`. `drive` resumes those two phases from persisted `cancel-pending` state. See [inspect and cancel runtimes](../guides/runtime-inspection-cancellation.md).
