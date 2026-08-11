## Why

Cache operations and garbage collection are currently grouped under a remote administration namespace even though cache is the user-facing resource and GC has both local and remote modes. A direct cache namespace and one source-selectable GC operation make the Python and generated CLI surfaces smaller, clearer, and consistent with the rest of `Dml`.

## What Changes

- Add `dml.cache.get(cache_key)` for resolving a cached DAG and `dml.cache.invalidate(*cache_keys)` for invalidating exact cache keys.
- Add top-level `dml.gc(*, remote: bool = False) -> LocalGCSummary | RemoteGCSummary`; local GC remains the default and `remote=True` runs maintenance only against configured `remote.root`.
- Generate `dml cache get`, `dml cache invalidate`, and `dml gc [--remote]` from the revised shared signatures.
- Remove the `dml.admin.remote` namespace entirely and remove local GC from `dml.admin`.
- Keep dependency endpoints import-only; `dml.gc(remote=True)` does not accept or collect a dependency endpoint.
- **BREAKING**: remove `dml.admin.remote.get_cache`, `dml.admin.remote.invalidate_cache`, `dml.admin.remote.gc`, and `dml.admin.gc` without compatibility aliases.
- **BREAKING**: remove the corresponding generated `dml admin remote ...` and `dml admin gc` commands; callers migrate to `dml cache ...` and `dml gc`.
- **BREAKING**: the previously specified `dml admin gc --dry-run` surface is removed rather than carried into `dml.gc`; the new GC method exposes only the `remote` source selector.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `unified-dml-surface`: Replace admin remote/cache/GC entrypoints with a top-level cache namespace and source-selectable top-level GC method, including union result typing and subsystem delegation.
- `admin-cli-controls`: Move cache get/invalidation and local/remote GC out of `dml admin`, remove obsolete remote listing, and define the generated top-level command migration.

## Impact

- Shared namespace classes, properties, GC orchestration, annotations, and summary types in `src/daggerml/_core/dml.py`.
- Generated CLI command paths and help, without command-specific changes to `src/daggerml/_cli.py`.
- Public callers of the removed Python and CLI paths.
- Contract tests for API discovery, cache delegation, local/remote GC selection, result payloads, missing remote configuration, and removed commands.
- CLI reference, cache-refresh guidance, runtime/cache concepts, GC/remotes architecture, and error documentation.
- No persisted-data changes, remote protocol changes, new dependencies, or migration of stored cache/GC state.
