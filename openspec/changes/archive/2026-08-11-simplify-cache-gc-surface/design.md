## Context

See `proposal.md` for motivation. The existing shared surface groups cache lookup, cache invalidation, and remote GC in one admin remote namespace while local GC lives directly under admin. The underlying workflows are already separate: cache lookup delegates to remote refs, invalidation delegates to execution state, local GC computes repository roots before calling the database, and remote GC delegates to remote maintenance. Generated CLI paths come entirely from public signatures and namespace properties.

The current specs also contain stale admin cache/list/dry-run command contracts that do not match the implemented namespace. This change establishes one canonical surface rather than preserving aliases for either shape.

## Goals / Non-Goals

**Goals:**

- Make cache a first-class resource namespace with concise `get` and `invalidate` operations.
- Express local versus main-remote GC through one keyword-only boolean selector.
- Preserve existing local and remote GC summary payloads and annotate their union exactly.
- Let generated CLI discovery produce the new command tree without `_cli.py` routing changes.
- Remove stale and duplicate admin remote/cache/GC contracts.

**Non-Goals:**

- Changing cache identity, invalidation propagation, GC reachability, remote cleanup algorithms, or summary fields.
- Adding dependency endpoint GC, GC filtering, dry-run behavior, or compatibility aliases.
- Moving lower-level remote/cache/GC implementation classes or persisted data.
- Redesigning unrelated `dml.admin` commands.

## Decisions

### Replace the remote namespace with a cache namespace

Rename the shared namespace object around cache operations to `_CacheNamespace`, expose it as `Dml.cache`, and rename methods to `get` and `invalidate`. The namespace keeps only its owning `Dml` reference and delegates to the same remote and execution-state methods as today.

Alternative considered: expose `get_cache` and `invalidate_cache` under `dml.cache`. The repeated resource name adds no information once the namespace is canonical.

### Put GC at the top-level shared boundary

Move the existing local GC orchestration to a module-level helper and expose `Dml.gc(*, remote: bool = False)`. The default local path does not resolve or access remote configuration. The remote path uses the existing required-remote helper and remote GC implementation. No `dep` parameter is introduced because dependencies are import-only.

Alternative considered: `dml.admin.gc(remote=...)`. This retains an administration layer that the desired Git-like `dml gc` command does not need.

### Use a union of unchanged summary types

Rename the remote summary alias for clarity and annotate `Dml.gc` as `LocalGCSummary | RemoteGCSummary`. Both are `TypedDict` JSON-family outputs, so generated CLI serialization can use its existing structured-output path. The `remote` argument determines which member is returned.

Alternative considered: normalize both modes into one superset payload. That would invent nullable fields and break stable mode-specific output without improving callers that already know the selected mode.

### Remove old surfaces atomically

Delete `Dml.admin.remote`, `Dml.admin.gc`, and their generated commands in the same change that introduces replacements. Do not add forwarding aliases or deprecation wrappers.

Alternative considered: preserve compatibility aliases for one release. The repository has no demonstrated external compatibility requirement, and aliases would keep the namespace ambiguity this change resolves.

### Keep CLI behavior signature-driven

The public properties, signatures, docstrings, `Annotated` parameter help, and return types are sufficient for generating `cache get`, `cache invalidate`, and `gc --remote`. Implementation should update generator contracts but not add command-specific handling to `_cli.py`.

## Risks / Trade-offs

- [Existing Python and CLI callers break immediately] -> Document exact migrations and update all repository callers atomically; do not leave two canonical paths.
- [Union return annotations expose different payload keys] -> Keep each existing `TypedDict` unchanged and test mode-specific exact payloads and JSON serialization.
- [Local GC could accidentally require remote config] -> Branch on `remote` before constructing any remote-aware helper and test local GC without `remote.root`.
- [Cache invalidate with zero keys could become a silent no-op] -> Validate at least one exact key at the public boundary and test the failure.
- [Stale admin specs could survive archive] -> Include explicit removals and full modified requirement blocks in the delta specs.

## Migration Plan

1. Introduce the cache namespace and top-level GC method while updating focused Python contracts.
2. Remove admin remote/GC exposure and update generated CLI discovery tests in the same edit set.
3. Migrate repository docs and callers to `dml.cache` and `dml.gc`.
4. Run focused cache/GC/CLI tests followed by required type, lint, and non-slow suite checks.

Rollback restores the old namespace methods and command paths together. No persisted state or remote data requires rollback.
