# Executor Lifecycle

`ExecutorBase` defines these subclass methods:

| Method | Called when | Required behavior |
| --- | --- | --- |
| `resolve_runnable(uri, kwargs, sub)` | DAG lowering | validate and build a `Runnable` |
| `start(cache_key, execution_id, runnable, remote, scratch_uri)` | invoke with no adapter state | launch work or return terminal result |
| `poll(cache_key, execution_id, runnable, state, remote, scratch_uri)` | invoke with adapter state | idempotently check work using durable state |
| `cleanup(cache_key, execution_id, runnable, state, remote, scratch_uri, result_ref)` | cleanup after result publication | idempotently prune normally completed resources |
| `cancel(cache_key, execution_id, runnable, state, remote, scratch_uri, cancel_requested_by, argv_ptr=None)` | cancel operation | idempotently request backend stop |

`handle()` creates a new executor instance and dispatches based on `operation`
and adapter state. `poll()` is reached by repeated invoke requests; there is no
adapter `poll` operation. Store no live process state on `self`. Retry responses
must contain object state. Later calls for the same execution ID must act as
idempotent status checks.

The runtime invokes `cancel(...)` only after selecting the execution as
`cancel-pending`. Cancellation must remain idempotent because interrupted or
concurrent drivers can repeat the call. The runtime, not the executor, owns the
compare-and-swap from `cancel-pending` to `canceled`.

Invoke and cleanup use `success`, `retry`, or another nonempty failure code.
Retry may include a delay hint and requires durable state; failure requires
diagnostics. Cleanup returns retry while required finalization remains active,
must be harmless when repeated after a lost response, and must not publish a
result or change lifecycle. Normal teardown cannot live only in terminal
`poll()`, because independent result publication can stop further invokes.
Cancellation has its own response contract described in [Adapter
operations](adapter-operations.md).
