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

Cancellation first removes eligible cache pointers and selects the complete set
as `cancel-pending`. It then sends cancel operations concurrently with saved
state, `argv_ref`, and `requested_by`. Executors return `cancelled` only after
successful teardown, or return retry with durable state and an optional delay.
The runtime owns retries and the compare-and-swap to `canceled`.

The funk runtime publishes normal results independently and a coordinating
caller finalizes lifecycle. Invoke failure makes the caller synthesize an
adapter-error DAG. Cleanup receives the published result ref and must be
idempotent; cleanup retry or failure never changes or invalidates that result.
Extensions must not mutate execution objects or cache pointers.
