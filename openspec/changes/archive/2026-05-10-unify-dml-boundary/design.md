## Context

The original change was written as a full end-to-end boundary migration, but the branch only landed the first half of that story:

- `_internal.__init__` now lazy-exports a future shared `Dml` surface plus helper functions.
- `_internal.dml_context` now centralizes config-derived runtime/project helpers.
- Some `_internal` modules have started importing through the shared `_internal` export surface.

The missing part is the actual boundary itself. `_internal.__init__` references `daggerml._internal.dml` and `daggerml._internal.dml_resolution`, but those modules do not exist yet. That leaves the change marked complete while the shared internal orchestration boundary is still absent.

This design narrows the change to what the branch is actually ready for: finish the missing `_internal` boundary modules and wiring, keep the blast radius inside `src/daggerml/_internal/`, and defer broader API/CLI/contrib cleanup until a follow-up.

## Goals / Non-Goals

**Goals:**
- Make `_internal.Dml` and `_internal.dml_resolution` real and importable.
- Reuse the already-added `_internal.dml_context` helpers rather than moving context logic again.
- Keep the shared `Dml` orchestration boundary delegated to existing ops classes instead of re-implementing repository mechanics.
- Preserve the fixed namespaced method surface expected by current callers.
- Limit the remaining implementation to a few files under `src/daggerml/_internal/`.

**Non-Goals:**
- Removing `daggerml.api.Dml` or `DmlOps` compatibility surfaces in this change.
- Rewriting CLI handlers or contrib integrations.
- Expanding selector grammar beyond what is already specified elsewhere.
- Changing repository storage formats, remote schemas, or commit/tree semantics.

## Current State Snapshot

```text
callers/importers
       |
       v
daggerml._internal.__init__
       |
       +--> dml_context.py        [present]
       +--> ops/*                 [present]
       +--> dml_resolution.py     [missing]
       +--> dml.py / Dml          [missing]
```

## Decisions

### Treat the landed `_internal` work as groundwork, not completion

The existing `_internal.__init__` export expansion and `_internal.dml_context` module are part of the intended architecture and should be recorded as completed groundwork for this change rather than rolled back or ignored.

Rationale:
- Those files already establish the shape of the future boundary.
- They are useful once the missing `Dml` and resolution modules exist.
- Reframing them as groundwork makes the remaining plan honest about what is done and what is not.

Alternatives considered:
- Revert the groundwork and restart the change. Rejected because the landed context/export helpers are aligned with the intended end state.

### Finish the boundary with two new `_internal` modules and light wiring

The remaining implementation should add:

- `src/daggerml/_internal/dml_resolution.py` for revision and DAG-selector helpers.
- `src/daggerml/_internal/dml.py` for the shared context-managed `Dml` facade.
- Minimal adjacent wiring inside `_internal` if import cycles or export cleanup require it.

`Dml` should delegate config-derived context lookup to `dml_context` and repository actions to existing ops classes.

Rationale:
- The missing modules are the actual reason the boundary is incomplete.
- Adding them finishes the architecture already implied by `_internal.__init__`.
- Keeping the work inside `_internal` avoids reopening a wide migration while the core boundary is still absent.

Alternatives considered:
- Expand the remaining work back out to `api`, `_cli`, and `contrib` in the same change. Rejected because the missing `_internal` boundary is still the blocking prerequisite.

### Preserve compatibility wrappers until a follow-up cleanup

This change does not need to remove `daggerml.api.Dml` or `DmlOps` immediately. The important step now is to make `_internal.Dml` real and canonical so other layers can converge on it without importing missing modules.

Rationale:
- Current callers and contrib helpers still reference compatibility surfaces.
- Removing them now would expand the scope far beyond the missing `_internal` work.
- Once `_internal.Dml` exists, follow-up cleanup becomes mechanical instead of speculative.

Alternatives considered:
- Remove all compatibility entrypoints now. Rejected because it would require many extra file edits outside `_internal`.

### `Dml` orchestrates by delegating to the relevant subsystem

The new `_internal.Dml` should coordinate workflows by farming repository actions to the relevant lower-level subsystem instead of re-implementing repository mechanics itself.

Delegation matrix:

| `Dml` responsibility | Delegated owner |
| --- | --- |
| fuzzy revision and DAG-selector resolution | fuzzy-resolution submodule |
| current head, default branch, remote-uri, and related config-derived context | config submodule |
| `show`, `log`, `diff`, `merge`, `revert`, revision-scoped DAG-map inspection | `CommitOps` |
| branch and HEAD state reads/writes | `HeadOps` |
| exact DAG reads and DAG inspection payload assembly inputs | `DagOps` |
| runtime index creation, staging, execution, and commit finalization | `IndexOps` |
| node materialization and unrolling | `NodeOps` |
| cache invalidation and cache-backed runtime support | `CacheOps` |
| remote discovery, fetch/pull/push support, and remote maintenance | `RemoteOps` |
| local garbage collection | `GcOps` |

Examples:
- commit-oriented workflows such as `show`, `log`, `diff`, `merge`, `revert`, and revision-scoped DAG-map inspection delegate to `CommitOps`
- head and branch state workflows delegate to `HeadOps`
- DAG inspection and exact DAG reads delegate to `DagOps`
- runtime staging and commit-finalization workflows delegate to `IndexOps`
- node materialization delegates to `NodeOps`
- cache invalidation delegates to `CacheOps`
- remote discovery and maintenance delegates to `RemoteOps`
- local garbage collection delegates to `GcOps`

Rationale:
- This preserves the existing subsystem ownership boundaries.
- It keeps `Dml` focused on caller-facing workflow composition, not storage mechanics.

Alternatives considered:
- Re-implement commit, head, or index logic directly in `Dml`. Rejected because it would flatten subsystem boundaries and duplicate repository logic.

### The public boundary is fixed top-level porcelain plus namespaces

The shared `_internal.Dml` class should expose:

- top-level repository methods: `status`, `show`, `log`, `diff`, `checkout`, `branch`, `fetch`, `pull`, `push`, `merge`, `revert`
- domain namespaces: `dag`, `admin`, `runtime`, `config`
- exact-subsystem namespace: `ops`

`ops` is an intentional low-level escape hatch for exact subsystem objects such as `CommitOps`, `HeadOps`, `DagOps`, `NodeOps`, `IndexOps`, `CacheOps`, `RemoteOps`, `GcOps`, and `ConfigOps`. These objects remain publicly reachable under `dml.ops.*`, but they are not promoted to direct top-level `Dml` attributes.

Rationale:
- This keeps the main caller-facing model aligned with the redesigned CLI and the intended Python-facing domain surface.
- It still preserves access to exact subsystem contracts for wrappers, tests, and advanced integrations that need them.
- It avoids reintroducing legacy storage-oriented nouns as first-class top-level public entrypoints on `Dml`.

Alternatives considered:
- Expose raw subsystem factories (`commit`, `head`, `index`, etc.) directly on `Dml`. Rejected because it reintroduces storage-oriented mental models into the primary boundary.
- Hide all exact subsystem objects. Rejected because wrappers and internal integrations still need a sanctioned exact-input escape hatch.

### The constructor matches the current runtime/context plumbing

The shared `Dml` constructor should accept the root runtime override inputs already threaded through callers: project-home, remote-uri, user, and config-home context. Construction establishes the repository/runtime context, and methods resolve any additional omitted values through `dml_context` inside the method body.

Rationale:
- This lets CLI handlers instantiate `Dml` directly from global parsed args.
- It gives API wrappers the same context model instead of inventing a parallel constructor contract.

Alternatives considered:
- Separate CLI-only and API-only constructors. Rejected because it recreates two runtime entrypoints.
- Push repo/remote context into every method instead of the constructor. Rejected because it would make call sites noisy and duplicate context normalization.

### The fixed namespaced method table remains the boundary target

The unified class will expose the already-chosen method table:

- top level: `status`, `show`, `log`, `diff`, `checkout`, `branch`, `fetch`, `pull`, `push`, `merge`, `revert`
- `dag`: `list`, `get`, `checkout`, `delete`
- `admin.index`: `list`, `get`, `delete`
- `admin.cache`: `invalidate`
- `admin.remote`: `list`, `gc`
- `admin`: `gc`
- `runtime`: `create`, `describe`, `put_literal`, `put_import`, `start_fn`, `commit`
- `config`: `get`, `set`, `show`
- `ops`: `commit`, `head`, `dag`, `node`, `index`, `cache`, `remote`, `gc`, `config`

Rationale:
- This preserves the porcelain-vs-admin-vs-runtime split already chosen while making config explicit and keeping exact subsystem access under one intentionally low-level namespace.
- It keeps the CLI and Python-facing boundary aligned around one vocabulary.

Alternatives considered:
- Put config behavior on top-level `Dml` methods. Rejected because config is a distinct concern and already reads naturally as a namespace.

### Return values are JSON-ready payloads with typed leaves allowed

`Dml` methods will return plain dict/list/bool/int/str/None payloads for container structure, but leaf values may still include `Ref`, `Uri`, `Error`, and `Runnable` objects for shared encoding and wrapper use.

Rationale:
- This keeps CLI handlers thin: parse, call `Dml`, JSON-encode.
- It avoids premature stringification in the domain layer while keeping result shapes serialization-friendly.

Alternatives considered:
- Return rich result objects for porcelain methods. Rejected because it would force CLI-specific reshaping logic back into callers.
- Stringify all typed leaves inside `Dml`. Rejected because the codebase already has stable encoders for several typed leaf objects.

### Init and recovery live on the shared internal `Dml`

Repository bootstrap and recovery workflows should be exposed on the shared `_internal.Dml` class while preserving the existing config-first recovery behavior and using `dml_context` plus the relevant ops classes to perform the work.

Rationale:
- Removing `DmlOps` requires a new owner for bootstrap workflows.
- Init is part of the caller-facing repository boundary and fits the new role of `Dml`.

Alternatives considered:
- Leave a minimal `DmlOps` only for init. Rejected because it would preserve a second orchestration entrypoint after the rest of the class is removed.

### Implementation proceeds from shell to namespaces to porcelain

The implementation should be staged in dependency order rather than by broad feature bucket.

Recommended order:

1. establish the `Dml` shell with only `_context` and `_tempdirs`, plus context-manager lifecycle and private helper stubs
2. add the `ops` namespace so exact subsystem objects are available under one sanctioned low-level entrypoint
3. add the `config` namespace because it is thin, mostly delegated, and validates the namespace pattern early
4. add the `runtime` namespace because active DAG runtime workflows are central to wrapper compatibility and depend mostly on `IndexOps`
5. add `dml_resolution.py` so revision and DAG-selector behavior is centralized before higher-level caller-facing namespaces rely on it
6. add the `dag` namespace on top of resolution plus existing DAG/node ops
7. add the `admin` namespace after the underlying subsystem entrypoints already exist
8. add the top-level porcelain workflows last, reusing the namespaces and resolution helpers rather than inventing parallel paths
9. add bootstrap/recovery flows (`create`, `temporary`, `init`) once the surrounding config/ops plumbing is already in place

Rationale:
- This reduces circular design pressure while the shared boundary is still being formed.
- It validates the public namespace model before layering full repository porcelain on top.
- It keeps `Dml` itself small by forcing most behavior into namespaces or delegated helpers first.

Alternatives considered:
- Implement top-level porcelain first. Rejected because it encourages one-off helper logic before the namespace model is stable.
- Start with init/recovery first. Rejected because bootstrap touches too many surrounding concerns to be the clean first landing point.

## Risks / Trade-offs

- [The branch already imports missing modules] → Finish the missing modules first and keep the remaining patch small.
- [Temporary duplicate entrypoints] → Keep `api.Dml` and `DmlOps` as compatibility surfaces until a follow-up cleanup change.
- [Overgrown `Dml`] → Keep namespace boundaries explicit and keep transactional storage logic in ops classes.
- [Import-cycle risk from `_internal.__init__`] → Limit new wiring to the minimum needed for the shared boundary to import cleanly.
