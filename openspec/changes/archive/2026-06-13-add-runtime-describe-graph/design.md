## Context

Runtime execution lineage is currently split between local index discovery and remote execution-state storage. Open runtime indexes are discoverable through `Dml.runtime.list()`, while nested execution lifecycle and caller-owned spawned summaries live in `exec_state.py` execution records under `exec/state/*.json`.

That execution state is enough to coordinate launch, resume, cancellation, and invalidation, but it does not currently expose a runtime-owned descendant graph payload. The active spawned list is also transient: once a child completes, the parent drops that id from `spawned_execution_ids`, which makes completed descendants invisible to later lineage inspection.

This change adds a public runtime graph query without pulling DAG objects, launch state, cache refs, or active refs. The graph is intentionally defined as an execution-record projection rather than a mixed execution-plus-DAG inspection surface.

## Goals / Non-Goals

**Goals:**
- Expose `Dml.runtime.describe_graph(*roots: Ref | str)` as the public runtime inspection entrypoint.
- Default empty public input to all currently open local runtime indexes.
- Keep all graph traversal and payload shaping logic inside `exec_state.py` after root-id collection.
- Preserve completed descendant lineage by extending execution records with durable `child_execution_ids` and `created_at`.
- Return one payload entry per reachable execution using only execution-record data.

**Non-Goals:**
- Hydrating DAGs, launch state, active refs, cache refs, or adapter scratch objects.
- Returning unrelated executions that are not descendants of the requested roots.
- Changing invalidation or caller-edge storage to power this query.
- Defining visualization or presentation helpers beyond the raw data payload.

## Decisions

### `describe_graph` is a runtime namespace method, but execution-state owns the behavior

The public API will live on `Dml.runtime` because callers already discover open indexes and runtime controls there. The runtime layer will only normalize inputs:

- explicit `Ref | str` roots become execution-id strings
- empty input becomes the ids of `dml.runtime.list()` entries

After that normalization, `exec_state.py` owns the full operation: record loading, recursion, and payload shaping.

Alternative considered:
- Put traversal logic in `dml.py` and call `ExecutionState` only for point reads. Rejected because it would spread execution-state semantics across two layers and make future record changes harder to contain.

### The graph is derived only from execution records

The returned payload will be built exclusively from `exec/state/<execution_id>.json` objects. It will not read DAGs, launch state, edge files, cache refs, or active refs.

This keeps the graph query narrowly scoped to runtime-owned lifecycle metadata and avoids surprising coupling to local DB availability or remote manifest hydration.

Alternative considered:
- Enrich the payload with DAG ids, launch state, active/cache metadata, or edge-derived caller information. Rejected because the immediate use case is lineage monitoring, not deep execution debugging, and the extra data would broaden the contract unnecessarily.

### Execution records gain two distinct child summaries

Execution records will distinguish between:

- `spawned_execution_ids`: active direct descendants still in flight
- `child_execution_ids`: completed direct descendants retained for lineage monitoring

When a direct child reaches a terminal lifecycle, the runtime will remove it from `spawned_execution_ids` and add it to `child_execution_ids`. The two sets must remain deduped and disjoint.

Alternative considered:
- Reuse `spawned_execution_ids` as a historical list. Rejected because cancellation traversal wants the active frontier, while graph monitoring wants durable lineage, and merging those semantics would make both meanings ambiguous.

### Graph traversal follows both active and completed descendants

Starting from the normalized root execution ids, traversal recurses through both `spawned_execution_ids` and `child_execution_ids`. The payload includes only the reachable closure rooted at those ids, with each execution included at most once.

This satisfies two requirements at once:

- active descendants remain visible while still running
- inactive descendants remain visible after completion

Alternative considered:
- Traverse only `child_execution_ids` and treat `spawned_execution_ids` as operational-only metadata. Rejected because that would hide currently active descendants from the graph.

### Missing roots should fail loudly

If a requested root execution id has no execution record, the graph query should fail instead of silently omitting that root. The graph contract is rooted in exact execution ids, whether caller-provided or derived from local indexes, so a missing root indicates stale state or corruption rather than a normal absence.

Alternative considered:
- Silently skip missing roots. Rejected because it produces incomplete graphs that look valid.

## Risks / Trade-offs

- Record schema expansion affects persisted remote state -> Keep the new fields additive and preserve existing lifecycle meanings so older records fail in obvious ways during development rather than silently mis-shaping payloads.
- Parent/child transition bugs could duplicate ids across `spawned` and `children` -> Centralize the transition in one helper that removes from spawned and adds to children atomically at the record-update level.
- Graph traversal can surface stale descendant ids if a record was partially written before failure -> Keep traversal strict for root records and test recursive behavior with mixed active and terminal descendants.
- Public method accepts both `Ref` and `str`, which can blur exact-vs-selector semantics -> Restrict the method contract to execution-root ids only and normalize to plain execution-id strings immediately.

## Migration Plan

This is an additive runtime inspection change with no separate deployment phase in this repository.

Implementation sequence:
- expand execution-record schema and creation paths
- update spawned-to-child transition logic for terminal descendants
- add execution-state graph extraction
- expose `Dml.runtime.describe_graph(*roots)`
- add contract/integration coverage for empty-root defaulting and descendant-only traversal

Rollback is straightforward: remove the public method and stop reading the additive fields. Existing extra JSON fields in remote execution records are backward-tolerant for development environments because they do not change object identity or path layout.

## Open Questions

- None at proposal time. The public name, payload shape, traversal scope, and storage boundary are already decided.
