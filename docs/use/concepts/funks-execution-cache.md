# Funks, execution, and cache

A funk is a DaggerML-packaged `Runnable`. Adding one to a DAG gives DaggerML the information it needs to run it. The current authoring tooling packages Python functions with `@api.funkify`, but a funk is not inherently tied to Python. Calling a funk from a DAG records DaggerML data for the computation and creates a result node. Its execution produces another DAG, so inputs, result, and failure can all be inspected later.

For a non-builtin funk, DaggerML first checks the cache identity derived from the staged runnable and normalized DaggerML data. The durable cache pointer names the current execution attempt; public cache lookup returns its result only after the unified execution record contains a reusable terminal `result_ref`. Distributed execution and cache coordination require `remote.root`.

Use `Dml.cache.get(cache_key)` to resolve the current cached DAG.
`Dml.cache.describe(cache_key)` inspects the current pointer and returns its
execution `Ref`, lifecycle, and reusable terminal DAG `Ref` when one exists.
To intentionally invalidate work, pass one or more exact `index:` or
`frozenindex:` execution `Ref` values to `Dml.cache.invalidate`, rather than
cache keys. Invalidation follows eligible execution lineage and affects every
user sharing the same `remote.root`.

Use [author a DAG](../guides/author-a-dag.md) for the current Python authoring pattern and [refresh cached work](../guides/refresh-cache.md) when intentionally recomputing a result.
