# Funks, execution, and cache

A funk is a DaggerML-packaged `Runnable`. Adding one to a DAG gives DaggerML the information it needs to run it. The current authoring tooling packages Python functions with `@api.funkify`, but a funk is not inherently tied to Python. Calling a funk from a DAG records DaggerML data for the computation and creates a result node. Its execution produces another DAG, so inputs, result, and failure can all be inspected later.

For a non-builtin funk, DaggerML first checks the cache identity derived from the staged runnable and normalized DaggerML data. The durable cache pointer contains only the current execution ID and is published after that execution's immutable `metadata.json`, semantic `state.json`, and coordinating `driver.json` exist. Legacy unified records and partial split records are stale rather than migrated. Public cache lookup returns a terminal `succeeded` or `failed` result only when `state.result_ref` is populated and cancelation or invalidation does not block reuse. Distributed execution and cache coordination require `remote.root`.

Normal completion cleanup is independent from result reuse. A cache-backed caller offers eligible pending cleanup one coordinated drive before returning, but a retry-delayed or failed cleanup does not invalidate the DAG. Adapter `retry` responses store a shared `driver.not_before`, so all callers back off together. Cancellation is separate: it conditionally removes a selected execution's matching cache pointer before requesting cancellation teardown. `cancel-pending` means cancellation owns the attempt and blocks further result mutation; `canceled` means the runtime completed that cancellation step or found no applicable adapter work.

Use `Dml.cache.get(cache_key)` to resolve the current cached DAG.
`Dml.cache.describe(cache_key)` inspects the current pointer and returns its
execution `Ref`, lifecycle, and reusable terminal DAG `Ref` when one exists.
To intentionally invalidate work, pass one or more exact `index:` or
`frozenindex:` execution `Ref` values to `Dml.cache.invalidate`, rather than
cache keys. Invalidation follows eligible execution lineage and affects every
user sharing the same `remote.root`.

Use [author a DAG](../guides/author-a-dag.md) for the current Python authoring pattern and [refresh cached work](../guides/refresh-cache.md) when intentionally recomputing a result.
