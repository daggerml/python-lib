# Refresh cached work

DaggerML reuses completed remote-backed computations by cache key. The key identifies normalized DaggerML data for a runnable computation, not a Python call. To intentionally recompute known work, first resolve the key's current execution, invalidate that execution ref, then run the authoring code again.

```bash
dml cache get CACHE_KEY
dml cache describe CACHE_KEY
dml cache invalidate index:<execution-id>
```

`cache describe` returns JSON with `execution`, `dag`, and `lifecycle`. Copy its
`execution` value; `dag` is null unless that execution is an unmarked reusable
terminal result. Cache invalidation requires `remote.root`. It affects that
execution and eligible callers for all users of the same remote, so confirm the
returned identity and coordinate with collaborators. The runtime graph or
execution record can help identify the relevant execution and cache key.
