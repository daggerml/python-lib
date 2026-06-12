## Context

Today the mutable runtime DAG hangs off `Commit.dag`, even when that commit is only serving as the current head for an open index. That leaks workspace state into immutable history objects and forces commit descriptions, serialization, and tests to treat commits as if they own a current in-progress DAG. The code paths in `types.py`, `index.py`, `commit.py`, and `dml.py` show that the actual mutable owner is the index workflow: index mutation updates `ctx.commit.dag`, runtime describe reads `idx.head -> commit.dag`, and commit descriptions surface `dag` even though named historical DAG state already lives in `Tree.dags`. The current `IndexOps.commit() -> commit_ref` contract also conflates two outcomes that should stay distinct: finalizing a DAG artifact and recording a named history update.

This change needs to preserve two distinct repository roles:

- commits remain immutable history records rooted in `parents`, `tree`, metadata, and timestamps,
- indexes remain mutable workspaces that can advance an in-progress DAG until finalization.

It also needs to preserve execution completion behavior for unnamed function DAGs that are finalized for cache/publication purposes without becoming named entries in a commit tree.

## Goals / Non-Goals

**Goals:**

- Make `Index` the only persisted owner of the current mutable DAG pointer.
- Remove `dag` from `Commit` data, validation, serialization, and commit-facing descriptions.
- Keep named historical DAG visibility rooted in `Tree.dags`.
- Preserve runtime mutation flows for literals, imports, builtin calls, adapter calls, and DAG finalization.
- Preserve execution completion and remote-cache publication for unnamed DAGs without reintroducing a commit-level current-DAG field.
- Make unnamed finalization leave the commit tree and `HEAD` unchanged.
- Update tests and inspection payloads to match the new ownership boundary.

**Non-Goals:**

- Redesign the commit/tree/history model beyond removing `Commit.dag`.
- Add compatibility shims that continue exposing `commit.dag` in commit payloads.
- Change how named DAGs are recorded in `Tree.dags`.
- Rework remote object rooting or cache identity beyond the minimum needed to stop depending on `Commit.dag`.

## Decisions

### Move the mutable DAG pointer from `Commit` to `Index`

`Index` should gain a `dag: Ref` field and `Commit` should lose its `dag` field entirely. `Index.head` continues to point at the immutable base-or-finalized commit for history purposes, while `Index.dag` points at the mutable in-progress DAG snapshot currently being edited.

This matches the existing architectural split: indexes are mutable workspaces, commits are immutable history snapshots.

Alternative considered: keep `Commit.dag` and treat it as "current DAG only when the commit is index-backed." Rejected because that preserves the blurred ownership model and keeps commit descriptions and serializers responsible for mutable state.

### Resolve commit context and index context differently

`TxnWithValid.get_ctx()` should stop assuming every commit-like context has a DAG. When called with an index ref, it should resolve the index, its head commit, its tree, and the index-owned DAG. When called with a commit ref, it should resolve only the commit and tree, leaving `dag` absent or `None`.

This keeps read paths honest: commit-oriented code can no longer accidentally depend on a mutable DAG pointer, while index-oriented code still gets the working DAG it needs.

Alternative considered: keep `get_ctx()` behavior unchanged by synthesizing a DAG from commit state. Rejected because unnamed commits would still need special handling and the helper would keep hiding the real ownership boundary.

### Finalized DAG publication stays split between tree state and execution side effects

When `IndexOps.commit()` finalizes a DAG:

- the finalized DAG ref comes from `Index.dag`,
- named DAG publication still happens by updating `Tree.dags[name]`,
- a commit object is created only when `name is not None`,
- unnamed execution completion uses the finalized DAG ref directly for `finish_execution(...)` and related cache publication.

This preserves the existing distinction between named repository history and unnamed execution artifacts. Unnamed finalization produces a DAG artifact without creating or advancing a history commit.

Alternative considered: force every finalized DAG into the commit tree, even unnamed execution DAGs. Rejected because it changes repository semantics beyond the requested ownership cleanup.

### `IndexOps.commit()` returns DAG identity plus optional history identity

`IndexOps.commit()` should return `(dag_ref, commit_ref | None)`.

- `dag_ref` is always returned because finalization always produces a durable DAG artifact.
- `commit_ref` is returned only when `name is not None` and the finalized DAG is published into `Tree.dags`.

`Dml.runtime.commit()` then becomes the layer that decides whether to merge/update `HEAD`:

- if `commit_ref is not None`, it performs the existing merge and branch or detached-HEAD update flow,
- otherwise it leaves history unchanged and returns the finalized `dag_ref` directly.

This keeps `IndexOps` responsible for index finalization while keeping `Dml.runtime.commit()` responsible for repository-head movement.

### Commit-facing descriptions become tree-rooted metadata only

`CommitOps.describe()`, `CommitOps.log()`, and any commit-facing JSON payloads should report commit metadata and tree-derived DAG maps, but omit `commit.dag`. Any surface that still needs a current DAG for an open runtime index should read it from `Index.dag` instead of from the index head commit.

Alternative considered: keep a derived `dag` field in descriptions for convenience. Rejected because it would preserve the old conceptual leak in user-visible outputs.

## Risks / Trade-offs

- [Unnamed finalized DAGs could become unreachable if code still assumes `commit.dag` or a returned commit ref] -> Audit commit finalization, execution completion, remote publication, and API callers to ensure they use the finalized `dag_ref` directly before index deletion.
- [Shared helpers may silently mix commit and index contexts] -> Narrow `get_ctx()` semantics and update callers explicitly rather than preserving ambiguous fallback behavior.
- [Payload changes break tests and downstream internal callers] -> Update commit/log/show/runtime tests in the same change and treat the output break as intentional.

## Migration Plan

1. Update `Commit` and `Index` types plus any typed helper/context loaders so the current DAG is loaded from indexes, not commits.
2. Rewrite `IndexOps` mutation and finalization paths to persist the working DAG on the index, return `(dag_ref, commit_ref | None)`, and use `dag_ref` for execution completion.
3. Update `Dml.runtime.commit()` to return the finalized DAG ref and only merge/update `HEAD` when `commit_ref is not None`.
4. Remove `dag` from commit description payloads and update runtime/index describe paths to source current-DAG data from indexes.
5. Update tests that assert `Commit.dag`, commit-facing `dag` payloads, or a commit-returning runtime commit API.
6. Run the relevant targeted tests plus the required finish checks to catch any remaining hidden dependency on `Commit.dag`.

## Open Questions

- None.
