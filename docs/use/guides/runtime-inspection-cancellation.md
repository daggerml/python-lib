# Inspect and cancel runtimes

Find open runtimes before diagnosing or stopping active work:

```bash
dml runtime list
dml runtime describe index:<execution-id>
dml runtime describe-graph index:<execution-id>
```

Cancel a runtime with the reference returned by `list`:

```bash
dml runtime cancel index:<execution-id> --max-retries 3
```

Use the complete `index:` or `frozenindex:` ref returned by `runtime list`, not its bare ID. The generated CLI constructs the `Ref`; direct Python calls must pass a `Ref` themselves.

In Python, name that identity with the `execution` parameter:

```python
session.runtime.cancel(execution=execution_ref, max_retries=3)
```

Cancellation has two phases. It first walks spawned execution state, preserves work with another caller, and marks the complete unreferenced set `cancel-pending`. Only after selection finishes does it invoke selected adapters concurrently. A `retry` response persists adapter state and `not_before`; only `cancelled` advances an execution to `canceled`.

The call waits until all selected executions are canceled or the retry budget is exhausted. Exhausted work remains `cancel-pending`, and calling `cancel` again resumes it with a fresh budget. Canceling an execution that finished before selection is a successful no-op.
