# Write An Executor

An executor class derives from `ExecutorBase`, declares `name` and `adapter`,
and implements `resolve_runnable()`, `start()`, and `poll()`. Implement
`cancel()` when the backend can stop work; the base implementation explicitly
reports that cancellation is unsupported.

1. Validate executor kwargs and nesting in `resolve_runnable()`, then return a
   `Runnable` with the target URI, normalized kwargs, nested `sub` if any, and
   the adapter executable.
2. In `start()`, either return a terminal invoke result or return `running` with
   durable launch state.
3. In `poll()`, use only the supplied state to find the in-flight work and
   return `running` or a terminal invoke result.
4. In `cancel()`, use saved state to stop the job and return
   `{"status": "cancelled", "error": None}` when the request has been
   accepted or completed.

Do not retain lifecycle state on the executor instance: `handle()` constructs a
new instance for every operation. Do not assume cancellation is synchronous or
that an active pointer still exists. Preserve nested adapter payloads when
wrapping work so they receive the original cache key, execution ID, remote,
scratch URI, state, and cancel fields.

`gc(...)` is optional and currently a no-op in the base class; the current
runtime does not dispatch it. Cleanup required for terminal or cancel paths
should be performed by the executor's own poll or cancel implementation.
