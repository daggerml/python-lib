# Refresh cached work

DaggerML reuses completed remote-backed computations by cache key. The key identifies normalized DaggerML data for a runnable computation, not a Python call. To intentionally recompute known work, invalidate its cache key, then run the authoring code again.

```bash
dml cache get CACHE_KEY
dml cache invalidate CACHE_KEY
```

Cache invalidation requires `remote.root`. It affects that computation identity for all users of the same remote, so confirm the key and coordinate with collaborators. The runtime graph or execution record can help identify the relevant execution and cache key.
