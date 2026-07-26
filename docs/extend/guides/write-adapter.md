# Write An Adapter

Use an adapter when an executor must be reached through a different transport.
For a backend that only needs local dispatch, add an executor to `LocalAdapter`
instead of creating an adapter.

1. Define a class with a stable `name`, `resolve_runnable(...)`, and
   `send(**payload)`.
2. Make `resolve_runnable()` return the concrete `Runnable` for the executor.
3. Make `send()` process exactly one request and return a JSON-compatible
   response. It receives `operation`, `runnable`, `cache_key`, `execution_id`,
   `remote`, `state`, and `scratch_uri`; cancel requests also include
   `requested_by` and `argv_ptr`.
4. If exposing a command, reuse or match `AdapterBase.cli()`: it reads a JSON
   request from stdin, a file, or S3 and writes one JSON response. `--poll`
   repeatedly invokes only invoke requests; it cannot coordinate cancellation.
5. Publish the class in `daggerml.contrib.adapters`.

`send()` should not mutate runtime cache or execution records. Return invoke
responses with `running`, `succeeded`, or `failed`; only cancel requests return
`cancelled` or `failed`. Validate remote transport errors before returning a
success result.

For a local adapter, follow `LocalAdapter.send()`: find the executor using the
runnable target URI and delegate the unchanged payload to `handle(...)`.
