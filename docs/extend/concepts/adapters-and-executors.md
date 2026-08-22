# Adapters And Executors

Adapters receive JSON-compatible runtime requests and return one JSON-compatible
response. `LocalAdapter` finds an executor by `(adapter, target URI)` and calls
its `handle(...)`; `LambdaAdapter` sends the payload to the target Lambda
function using synchronous `RequestResponse` invocation.

`ExecutorBase.handle(...)` routes `operation="invoke"` with
`adapter_state=None` to `start(...)`, and an invoke with saved adapter state to
`poll(...)`. This `poll()` is an executor method, not a wire operation. It routes
`operation="cleanup"` to `cleanup(...)` and `operation="cancel"` to
`cancel(...)` regardless of state.

For an asynchronous executor, `start(...)` returns:

```python
{"status": "retry", "error": None, "adapter_state": durable_state, "retry_after_ms": 1000}
```

Later invoke requests receive the latest adapter state stored by the runtime.
Retry responses require object state, and every call must be idempotent for its
execution ID. A synchronous operation returns `success`; another nonempty
status is a failure code and requires diagnostics. Shared `driver.not_before`
backpressure prevents concurrent callers from hammering the backend.

Cancellation is best-effort. The runtime can delete the execution's cache
pointer before the adapter confirms cancellation, so the underlying job
can still run briefly. A cancel operation receives the saved state, the
execution-owned `argv_ref`, and `requested_by`; it must return `cancelled` or
`failed`, not an invoke result. The runtime sends cancel operations only after
it has selected the complete cancellation set as `cancel-pending`, and it owns
the compare-and-swap to `canceled`. Executors do not persist lifecycle state.

The funk runtime publishes normal results independently and a coordinating
caller finalizes lifecycle. Invoke failure makes the caller synthesize an
adapter-error DAG. Cleanup receives the published result ref and must be
idempotent; cleanup retry or failure never changes or invalidates that result.
Extensions must not mutate execution objects or cache pointers.
