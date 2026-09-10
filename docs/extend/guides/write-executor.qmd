# Write An Executor

An executor class derives from `ExecutorBase`, declares `name` and `adapter`,
and implements `resolve_runnable()`, `start()`, `poll()`, and idempotent
`cleanup()`. Implement
`cancel()` when the backend can stop work; the base implementation explicitly
reports that cancellation is unsupported.

1. Validate executor kwargs and nesting in `resolve_runnable()`, then return a
   `Runnable` with the target URI, normalized kwargs, nested `sub` if any, and
   the adapter executable.
2. In `start()`, return `success`, `retry` with durable adapter state and an
   optional delay hint, or a failure code with diagnostics.
3. In `poll()`, use only the supplied state to find the in-flight work and
   return the same response forms. This method is reached through repeated
   invoke requests, not a poll operation.
4. In `cleanup()`, use saved state and `result_ref` to prune normally completed
   resources. Retry while required finalization is active; repeated success
   must be harmless and cleanup must not change lifecycle or publish a result.
5. In `cancel()`, use saved state to stop the job and return
    `{"status": "cancelled", "error": None}` when the request has been
    completed successfully. Return retry with durable state and an optional
    delay while cancellation remains incomplete.

Do not retain lifecycle state on the executor instance: `handle()` constructs a
new instance for every operation. A cancel invocation is synchronous, may be
retried, and cannot assume a cache pointer still names the execution. Preserve nested adapter payloads when
wrapping work so they receive the original cache key, execution ID, remote,
scratch URI, state, and cancel fields.

Do not make terminal `poll()` the only normal teardown path: the funk runtime
may publish its result before another invoke observes backend termination.
