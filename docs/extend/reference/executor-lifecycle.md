# Executor Lifecycle

`ExecutorBase` defines these subclass methods:

| Method | Called when | Required behavior |
| --- | --- | --- |
| `resolve_runnable(uri, kwargs, sub)` | DAG lowering | validate and build a `Runnable` |
| `start(cache_key, execution_id, runnable, remote, scratch_uri)` | invoke with no state | launch work or return terminal result |
| `poll(cache_key, execution_id, runnable, state, remote, scratch_uri)` | invoke with state | resume/check work using durable state |
| `cancel(cache_key, execution_id, runnable, state, remote, scratch_uri, cancel_requested_by, argv_ptr=None)` | cancel operation | request backend stop and return cancel response |
| `gc(cache_key, execution_id, remote, scratch_uri, state)` | optional hook | no-op unless overridden |

`handle()` creates a new executor instance and dispatches based on `operation`
and state. Store no live process state on `self`. The first `running` response
must contain everything a later process needs in `state`; later running states
are not a reliable way to update that persisted launch state.

Invoke results use `running`, `succeeded`, or `failed`, with `error`, `state`,
ID. Cancellation has its own two-status response contract described in
[Adapter operations](adapter-operations.md).
