# Write An Adapter

Use an adapter when an executor must be reached through a different transport.
For a backend that only needs local dispatch, add an executor to `LocalAdapter`
instead of creating an adapter.

1. Define a class with a stable `name`, `resolve_runnable(...)`, and
   `send(**payload)`.
2. Make `resolve_runnable()` return the concrete `Runnable` for the executor.
3. Make `send()` process exactly one request and return a JSON-compatible
   response. It receives `operation`, `runnable`, `cache_key`, `execution_id`,
    `remote`, `adapter_state`, and `scratch_uri`; cleanup adds `result_ref`, and
    cancel adds `requested_by` and `argv_ref`.
4. If exposing a command, reuse or match `AdapterBase.cli()`: it reads a JSON
   request from stdin, a file, or S3 and writes one JSON response. `--poll`
    repeats invoke retries and then drives nested cleanup. It never sends a
    protocol-level poll request and cannot coordinate cancellation.
5. Publish the class in `daggerml.contrib.adapters`.

`send()` should not mutate runtime cache or execution objects. Invoke and
cleanup return `success`, `retry` with object continuation state and optional
`retry_after_ms`, or a nonempty failure code with diagnostics. Cleanup must
forward published-result context. Cancel requests retain their separate
contract. Validate remote transport errors before returning success.

For a local adapter, follow `LocalAdapter.send()`: find the executor using the
runnable target URI and delegate the unchanged payload to `handle(...)`.
