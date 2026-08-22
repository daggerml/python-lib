# Executor Lifecycle

`ExecutorBase` defines these subclass methods:

| Method | Called when | Required behavior |
| --- | --- | --- |
| `resolve_runnable(uri, kwargs, sub)` | DAG lowering | validate and build a `Runnable` |
| `start(cache_key, execution_id, runnable, remote, scratch_uri)` | invoke with no adapter state | launch work or return terminal result |
| `poll(cache_key, execution_id, runnable, state, remote, scratch_uri)` | invoke with adapter state | idempotently check work using durable state |
| `cancel(cache_key, execution_id, runnable, state, remote, scratch_uri, cancel_requested_by, argv_ptr=None)` | cancel operation | idempotently request backend stop |
| `gc(cache_key, execution_id, remote, scratch_uri, state)` | optional hook | no-op unless overridden |

`handle()` creates a new executor instance and dispatches based on `operation`
and adapter state. Store no live process state on `self`. Running invoke
responses must contain object state; cancel responses may omit it. Later calls
for the same execution ID must act as idempotent status checks.

The runtime invokes `cancel(...)` only after selecting the execution as
`cancel-pending`. Cancellation must remain idempotent because interrupted or
concurrent drivers can repeat the call. The runtime, not the executor, owns the
compare-and-swap from `cancel-pending` to `canceled`.

Invoke results use `running`, `succeeded`, or `failed`, with `error`, `state`,
ID. Cancellation has its own two-status response contract described in
[Adapter operations](adapter-operations.md).
