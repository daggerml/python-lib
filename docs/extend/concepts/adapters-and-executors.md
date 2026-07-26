# Adapters And Executors

Adapters receive JSON-compatible runtime requests and return one JSON-compatible
response. `LocalAdapter` finds an executor by `(adapter, target URI)` and calls
its `handle(...)`; `LambdaAdapter` sends the payload to the target Lambda
function using synchronous `RequestResponse` invocation.

`ExecutorBase.handle(...)` routes `operation="invoke"` with `state=None` to
`start(...)`, and an invoke with saved state to `poll(...)`. It routes
`operation="cancel"` to `cancel(...)` regardless of state.

For an asynchronous executor, `start(...)` returns:

```python
{"status": "running", "error": None, "state": durable_state, "dag_id": None}
```

Later polls receive the launch state stored by the runtime. Treat that state as
immutable and sufficient to resume from a different process. A synchronous
executor may return a terminal result directly.

Cancellation is best-effort. The runtime can remove an execution from active
coordination before the adapter confirms cancellation, so the underlying job
can still run briefly. A cancel operation receives the saved state, the
execution-owned `argv_ptr`, and `requested_by`; it must return `cancelled` or
`failed`, not an invoke result.

The runtime publishes successful and failed cache entries after it observes an
invoke terminal result. Extensions must not publish those entries themselves.
