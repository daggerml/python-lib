# Inspect and cancel runtimes

Find open runtimes before diagnosing or stopping active work:

```bash
dml runtime list
dml runtime describe index:<execution-id>
dml runtime describe-graph index:<execution-id>
```

Cancel a runtime with the reference returned by `list`:

```bash
dml runtime cancel index:<execution-id> --mode full
```

Use the complete `index:` or `frozenindex:` ref returned by `runtime list`, not its bare ID. The generated CLI constructs the `Ref`; direct Python calls must pass a `Ref` themselves.

In Python, name that identity with the `execution` parameter:

```python
session.runtime.cancel(execution=execution_ref, mode="full")
```

Full cancellation has two phases. It first walks spawned execution state, preserves work with another caller, and marks the complete unreferenced set `cancel-pending`. Only after selection finishes does it invoke cancellation for the selected adapters and compare-and-swap each execution to `canceled`.

Use `--mode drive` to resume an interrupted cancellation already persisted as `cancel-pending`. Canceling an execution that finished before selection is a successful no-op. Cancellation is coordinated through remote execution state for remote-backed functions; inspect the graph again afterwards to see lineage and terminal state.
