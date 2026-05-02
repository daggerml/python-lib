## Context

Head and index pointer storage is currently handled in multiple internal ops modules. `CommitOps`, `IndexOps`, `RemoteOps`, and `BaseOps.get_ctx` all read or write `Head` and `Index` objects directly. That makes pointer lifecycle rules hard to enforce in one place, leaks storage refs outside `HeadOps`, and forces retryable stale-write handling to be reimplemented ad hoc.

The refactor needs to preserve current transaction behavior. Callers such as `IndexOps.commit`, `put_literal`, and `start_fn` must be able to run larger workflows in a caller-owned transaction while still routing pointer operations through `HeadOps` public methods.

## Goals / Non-Goals

**Goals:**
- Make `HeadOps` the only internal module that creates, reads, updates, or deletes branch/index pointers in storage.
- Hide `Head` and `Index` refs and objects from all non-`HeadOps` callers.
- Add compare-and-swap style branch/index commit updates using `update_branch_commit` and `update_index_commit`.
- Raise a dedicated retryable conflict error with a `current_commit` attribute when the expected commit no longer matches storage.
- Preserve single-transaction caller workflows by letting public `HeadOps` methods accept `txn=None`.

**Non-Goals:**
- Changing higher-level retry strategy or forcing callers to read and write in the same transaction.
- Redesigning DAG, commit, or adapter execution semantics.
- Preserving head/index ref exposure in API or CLI surfaces that can be simplified to branch names and opaque index ids.

## Decisions

### `HeadOps` owns all pointer persistence
All branch/index storage access will move behind `HeadOps` public methods. Other modules will work with branch names, opaque index ids, and commit refs only.

Alternative considered: leave reads in shared helpers and centralize only writes. Rejected because reads also leak `Head`/`Index` refs and make it harder to keep caller contracts uniform.

### Public methods accept optional `txn`
Each public method will accept `txn=None`. If a txn is provided, the method uses it. Otherwise it opens its own transaction and delegates to a private txn-required helper.

Alternative considered: require callers to use private helpers for shared transactions. Rejected because the public API should remain the only caller entry point.

### Commit updates use expected-current semantics
`update_branch_commit(name, old_commit, new_commit, txn=None)` and `update_index_commit(index_id, old_commit, new_commit, txn=None)` will only update storage when the current commit matches `old_commit`.

Alternative considered: blind setter methods. Rejected because callers like `put_literal`, `start_fn`, and `commit` need a precise stale-write signal to retry safely.

### Conflict reporting uses one dedicated repo error subclass
The stale-write path will raise a dedicated `DmlRepoError` subclass carrying only `current_commit`.

Alternative considered: separate branch/index conflict types or richer payloads. Rejected as unnecessary because callers already know the target and expected/new commits.

### Index creation is commit-based
`HeadOps` will expose `create_index(commit_ref, txn=None)` and keep internal ref generation private. Callers must supply the starting commit regardless of whether it originated from a branch or an argv-backed commit bootstrap flow.

Alternative considered: multiple `create_index_from_*` entry points. Rejected because they duplicate pointer allocation concerns and widen `HeadOps` responsibilities.

## Risks / Trade-offs

- Pointer contract churn across many callers -> Update `CommitOps`, `IndexOps`, `RemoteOps`, API, CLI, and tests in one change.
- Hidden ref assumptions in tests -> Convert assertions to branch names, index ids, and commit refs.
- `BaseOps.get_ctx` currently assumes head/index-like refs -> Replace or narrow that helper so it no longer exposes `Head` objects outside `HeadOps`.
- API compatibility drift during ref removal -> Keep external behavior stable while changing only internal handle types where possible.

## Migration Plan

- Add the new `HeadOps` public methods and conflict error.
- Move branch/index storage access in `CommitOps`, `IndexOps`, and `RemoteOps` to those methods.
- Remove or refactor shared helpers that expose `Head`/`Index` objects.
- Update API, CLI, and tests to stop carrying head/index refs.
- Run contract and integration coverage for commit/head/index flows.

## Open Questions

- Whether index ids should be raw ids or a small opaque token object at API boundaries.
- Whether `HeadOps` should expose `describe_branch` / `describe_index` helpers later, or keep the public surface minimal for this change.
