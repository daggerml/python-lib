## 1. Cache Namespace

- [x] 1.1 Replace `_RemoteNamespace` with `_CacheNamespace` and expose `Dml.cache`; implement `get(cache_key: str) -> Ref | None` by delegating to the existing cache reader and `invalidate(*cache_keys: str) -> InvalidationResponse` by delegating to existing execution-state invalidation with configured user identity.
- [x] 1.2 Enforce the public invalidation boundary: require at least one exact string cache key, reject ref/non-cache selector forms before remote mutation, and preserve existing missing-cache and invalidation response behavior.
- [x] 1.3 Add focused cache namespace contracts for present/absent lookup, exact multi-key delegation, empty input, invalid selector forms, return annotations, `Annotated` help metadata, and absence of old admin cache methods.

## 2. Source-Selectable Garbage Collection

- [x] 2.1 Rename the remote GC payload type to `RemoteGCSummary` without changing its fields, preserve `LocalGCSummary`, and add `Dml.gc(*, remote: bool = False) -> LocalGCSummary | RemoteGCSummary` with runtime-visible `Annotated` metadata.
- [x] 2.2 Move local GC orchestration into a module-level helper used by `Dml.gc`, preserving HEAD/local/fetched/frozen runtime roots and exact local summary fields; ensure the default path does not resolve or access remote configuration.
- [x] 2.3 Route `dml.gc(remote=True)` to existing configured remote maintenance, preserve exact remote summary fields, surface the established missing-`remote.root` error, and expose no dependency or dry-run parameter.
- [x] 2.4 Add GC contracts for local default selection without remote config, remote delegation and missing configuration, exact mode-specific payloads, union return typing, local reachability behavior, and absence of dependency/dry-run inputs.

## 3. Remove Old Surface And Regenerate CLI

- [x] 3.1 Remove `Dml.admin.remote` and `Dml.admin.gc` without aliases, retain unrelated admin commands, and update all repository Python callers and namespace expectations to the canonical cache/GC paths.
- [x] 3.2 Update generated CLI contracts so `cache get`, `cache invalidate`, and `gc --remote` are present; `admin remote`, `admin gc`, `gc --dep`, and `gc --dry-run` are absent; cache and both GC summary variants serialize through existing generated transports without editing `src/daggerml/_cli.py`.

## 4. Documentation And Validation

- [x] 4.1 Update Python/CLI references, cache-refresh guidance, execution/cache concepts, remotes/GC architecture, and errors with the breaking migration from admin remote/GC paths to `dml.cache` and `dml.gc`; remove stale ordinary remote and dry-run command references.
- [x] 4.2 Run focused cache, GC, reachability, and generated CLI tests, then complete the required validation sequence in order: `uv run --dev pyright`, `uv run --dev ruff check --fix .`, and `uv run --dev pytest -m 'not slow' .`; review Ruff edits and require every check to pass.
