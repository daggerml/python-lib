# Adapters And Executors

Adapters receive JSON-compatible runtime requests and return one JSON-compatible
response. `LocalAdapter` finds an executor by `(adapter, target URI)` and calls
its `handle(...)`; `LambdaAdapter` sends the payload to the target Lambda
function using synchronous `RequestResponse` invocation.

`ExecutorBase.handle(...)` routes `operation="invoke"` with
`adapter_state=None` to `start(...)`, and an invoke with saved adapter state to
`poll(...)`. It routes `operation="cancel"` to `cancel(...)` regardless of state.

For an asynchronous executor, `start(...)` returns:

```python
{"status": "running", "error": None, "adapter_state": durable_state, "dag_id": None}
```

Later polls receive the latest adapter state stored by the runtime. Running
invoke responses return object state and every call must be an idempotent check
for its execution ID; cancellation responses may omit state. A synchronous
executor may return a terminal result directly.

Cancellation is best-effort. The runtime can delete the execution's cache
pointer before the adapter confirms cancellation, so the underlying job
can still run briefly. A cancel operation receives the saved state, the
execution-owned `argv_ref`, and `requested_by`; it must return `cancelled` or
`failed`, not an invoke result. The runtime sends cancel operations only after
it has selected the complete cancellation set as `cancel-pending`, and it owns
the compare-and-swap to `canceled`. Executors do not persist lifecycle state.

The runtime publishes successful and failed cache entries after it observes an
invoke terminal result. Extensions must not publish those entries themselves.
