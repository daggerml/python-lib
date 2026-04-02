---
status: proposed
doc_type: spec
---

# Contrib Execution Graph

## Authority

This document is authoritative for the contrib runtime live execution-graph model, SQLite storage schema, caller identity rules, and root-driven cancellation and sweep behavior.

This document owns:

- live execution graph node and edge identities,
- the SQLite table schema for live contrib execution state,
- caller-resolution rules for graph edges,
- root-driven cancellation propagation,
- `dml contrib cancel <index-id>` semantics,
- `dml contrib sweep` semantics,
- live-only retention rules.

This document does not own:

- the core adapter-boundary payload or output schema,
- executor-specific launch details,
- backend serialization details outside the contrib runtime SQLite database,
- human-facing CLI formatting,
- historical retention of dead executions.

Runtime lifecycle callable semantics for adapters and executors are authoritative in [runtime-contract.md](runtime-contract.md). Shared execution-state field semantics and executor metadata conventions are authoritative in [executor-state.md](executor-state.md).

## Scope

In scope:

- the contrib runtime SQLite database used to track live roots and live executions,
- graph node ids for user-DAG roots and execution jobs,
- caller-edge creation and removal rules,
- graceful and forced cancellation timing,
- `dml contrib cancel <index-id>` behavior,
- `dml contrib sweep` behavior,
- live-only cleanup semantics.

Out of scope:

- archival and history tables,
- remote execution surfaces that do not participate in this runtime's heartbeat, cancel, and gc contract,
- CLI text rendering,
- non-SQLite backend variants.

## Purpose

Define one live execution graph owned by the contrib runtime so that:

- multiple callers can share the same execution safely,
- root cancellation removes only the ownership edges for that root,
- executions remain live only while still reachable from at least one user root,
- orphaned executions are canceled and cleaned up deterministically.

## Glossary

- Execution Graph: the live directed graph stored in the contrib runtime SQLite database.
- Root Node: a live `index:*` node representing a user-DAG caller.
- Execution Node: a live `cache:*` node representing one execution identified by a cache key.
- Caller Edge: a directed edge `src -> dst` meaning `src` is an active caller of `dst`.
- Orphan: a live execution node with zero incoming caller edges.
- Graceful Cancel: executor-specific best-effort termination without immediate force kill.
- Forced Cancel: executor-specific hard termination after the graceful deadline expires.
- Sweep: the contrib runtime maintenance pass that applies orphan cancellation, forced termination, cleanup, and live-graph consistency removal.

## Contract

### Node Identity

The execution graph contains exactly two node kinds:

- `index`:
  - id format: `index:<index-id>`
  - represents a live user-DAG root caller
- `cache`:
  - id format: `cache:<cache_key>`
  - represents one live execution job

Node ids are opaque strings to graph consumers except for the required `index:` and `cache:` prefixes.

`cache_key` is the canonical execution identity for contrib runtime execution nodes.

### Edge Semantics

A directed edge `src_id -> dst_id` means:

- `src_id` is an active caller of `dst_id`,
- `dst_id` remains live only while it is reachable from at least one `index:*` root through one or more caller edges.

Duplicate edges are not allowed.

### Caller Resolution

For each adapter invocation that creates or resumes a contrib execution node, caller identity is resolved in this order:

1. if `DML_CACHE_KEY` is set in the current process environment, caller id is `cache:<DML_CACHE_KEY>`
2. else if the adapter payload includes `parent_id`, caller id is `index:<parent_id>`
3. else the invocation has no graph caller

This precedence is mandatory.

`DML_CACHE_KEY` is authoritative for nested contrib-to-contrib execution calls because it identifies the currently running parent execution across process boundaries.

### Callee Resolution

For each contrib execution invocation, callee id is always:

- `cache:<cache_key>`

where `cache_key` is the execution cache key for the invoked job.

### Storage Ownership

The contrib runtime owns the execution-graph SQLite database.

The contrib runtime MUST be the only authority that:

- creates and removes graph nodes,
- creates and removes caller edges,
- records live execution heartbeat and cancel metadata in this database,
- performs orphan-driven cancellation and cleanup.

Executors and adapters MUST use contrib runtime state APIs for this graph. They MUST NOT define independent graph storage.

### SQLite Schema

The live execution graph database contains these tables.

#### `nodes`

```sql
create table nodes (
  id text primary key,
  kind text not null check (kind in ('index', 'cache')),
  adapter text,
  uri text,
  status text,
  error text,
  heartbeat_ts real,
  cancel_requested_ts real,
  metadata_json text not null default '{}'
);
```

Column rules:

- `id`:
  - required
  - primary key
- `kind`:
  - required
  - one of `index`, `cache`
- `adapter`:
  - `NULL` for `index` nodes
  - required for `cache` nodes
- `uri`:
  - `NULL` for `index` nodes
  - required for `cache` nodes
- `status`:
  - `NULL` for `index` nodes
  - for `cache` nodes, one of `pending`, `running`, `succeeded`, `failed`, `canceled` while the row remains live
- `error`:
  - nullable
  - execution error text for `cache` nodes when applicable
- `heartbeat_ts`:
  - nullable for `index` nodes
  - last heartbeat or update timestamp for `cache` nodes
- `cancel_requested_ts`:
  - nullable
  - set when orphan cancellation has been requested for a `cache` node and cleared only by row deletion
- `metadata_json`:
  - JSON object string
  - executor or runtime-specific live metadata
  - empty object for `index` nodes unless the runtime needs root-local metadata

`metadata_json` for `cache` nodes MUST contain only live runtime data needed for polling, cancellation, or cleanup, such as process ids, container ids, workdirs, or job handles.

Example rows:

```json
[
  {
    "id": "index:abc123",
    "kind": "index",
    "adapter": null,
    "uri": null,
    "status": null,
    "error": null,
    "heartbeat_ts": null,
    "cancel_requested_ts": null,
    "metadata_json": "{}"
  },
  {
    "id": "cache:7f9d2c",
    "kind": "cache",
    "adapter": "dml-local-adapter",
    "uri": "script",
    "status": "running",
    "error": null,
    "heartbeat_ts": 1775034123.25,
    "cancel_requested_ts": null,
    "metadata_json": "{\"pid\":43122,\"workdir\":\"/tmp/dml-script-7f9d2c-abcd\"}"
  }
]
```

#### `edges`

```sql
create table edges (
  src_id text not null,
  dst_id text not null,
  primary key (src_id, dst_id),
  foreign key (src_id) references nodes(id) on delete cascade,
  foreign key (dst_id) references nodes(id) on delete cascade
);

create index edges_dst_idx on edges(dst_id);
```

Column rules:

- `src_id`:
  - caller node id
- `dst_id`:
  - callee node id

An `edges` row exists only while that caller relationship is live.

Example rows:

```json
[
  {
    "src_id": "index:abc123",
    "dst_id": "cache:7f9d2c"
  },
  {
    "src_id": "cache:7f9d2c",
    "dst_id": "cache:8aa1ef"
  }
]
```

### Live-Only Retention

This database stores live objects only.

Caller edges are live-only as well. The contrib runtime MUST remove a caller edge once that caller is no longer actively waiting on the callee execution.

The contrib runtime MUST remove rows for executions after all of these are true:

- the execution is terminal,
- executor cleanup has completed,
- no live caller edges remain.

The contrib runtime MAY remove `index:*` root nodes once they have no outgoing edges and no remaining runtime purpose.

The contrib runtime MUST NOT rely on this database as a historical audit log.

### Execution Registration

For any in-scope contrib runtime execution node, the contrib runtime MUST ensure that the `cache:*` node exists before or during the first live runtime step for that execution.

For a `cache:*` node, the runtime MUST record:

- `adapter`
- `uri`
- normalized execution `status`
- `heartbeat_ts`
- live runtime metadata in `metadata_json`

Repeated polls or resumes for the same execution MUST update the existing `cache:*` node rather than create duplicate execution nodes.

### Edge Release

The contrib runtime MUST remove a caller edge when the caller is no longer actively blocked on that callee execution.

At minimum, this includes:

- after a caller observes that the callee has reached a terminal state,
- after executor cleanup for that callee has completed,
- when `dml contrib cancel <index-id>` removes a root caller's ownership edges.

The runtime MUST NOT retain caller edges merely for historical inspection.

### Edge Creation

When caller resolution returns a caller id, the contrib runtime MUST:

- ensure the caller node exists,
- ensure the callee node exists,
- insert edge `caller_id -> callee_id`,
- treat repeated creation of the same edge as idempotent.

Top-level invocation behavior:

- when caller resolution yields `index:<parent_id>`, the runtime MUST create or reuse that `index:*` node and insert `index:<parent_id> -> cache:<cache_key>`.

Nested invocation behavior:

- when caller resolution yields `cache:<DML_CACHE_KEY>`, the runtime MUST insert `cache:<parent_cache_key> -> cache:<child_cache_key>`.

### Liveness Rule

A `cache:*` node is live only while it is reachable from at least one `index:*` node.

Equivalently:

- any `cache:*` node with zero incoming caller edges is orphaned,
- orphaned execution nodes MUST be canceled and cleaned up by the contrib runtime.

### Cancellation

#### User Command

`dml contrib cancel <index-id>` targets root node `index:<index-id>`.

`cancel` MUST:

1. remove that root node's outgoing caller edges,
2. run orphan-cancellation propagation until no further changes occur.

`cancel` removes ownership from the targeted root. It does not directly force-cancel every descendant regardless of sharing.

#### Orphan Propagation

After root-edge removal, the contrib runtime MUST repeatedly:

1. identify live `cache:*` nodes with zero incoming edges,
2. for each such node:
   - if cancellation has not yet been requested, set `cancel_requested_ts`,
   - request graceful cancellation,
3. for each such node whose graceful deadline has expired and which is still non-terminal:
   - request forced cancellation,
4. for each such node that is terminal:
   - run executor cleanup,
   - remove that node's outgoing edges,
   - remove the node row,
5. repeat until the graph reaches a fixed point.

This propagation is the authoritative cancellation algorithm.

### Graceful and Forced Cancellation

The contrib runtime MUST attempt graceful cancellation before forced cancellation.

Graceful deadline:

- `cancel_requested_ts + 2 * HEARTBEAT_STALENESS`

If a node remains non-terminal after the graceful deadline, the contrib runtime MUST perform forced cancellation.

If the runtime determines that a node is already stale after cancellation was requested, it MAY proceed directly to forced cancellation.

Forced-cancel requests and cleanup MUST be idempotent.

### Sweep

`dml contrib sweep` is the contrib runtime live-state maintenance command.

`sweep` MUST:

- inspect the live graph,
- identify orphaned execution nodes,
- apply orphan propagation,
- enforce graceful-deadline forced cancellation,
- run cleanup for terminal live execution nodes,
- remove live rows that have become fully cleaned up.

`sweep` MAY also detect and handle live-state inconsistencies that can be resolved only by cancellation or deletion of live rows and live handles.

`sweep` MUST NOT invent or reconstruct historical state.

### Executor Requirements In Scope

For v1 scope, these requirements apply only to local and stateful contrib execution surfaces managed by this SQLite runtime.

In-scope executors with live runtime handles MUST support:

- `cancel(state)`:
  - best-effort graceful cancellation first,
  - forced termination when requested by the runtime,
  - idempotent behavior
- `gc(state)`:
  - removal of executor-owned live residue such as temp dirs, processes, containers, or job handles,
  - idempotent behavior

The runtime owns the cancellation decision and propagation order. Executors own executor-specific termination and cleanup mechanics.

### Adapter and URI Recording

For each `cache:*` node, `adapter` and `uri` identify the execution surface.

This document does not require all possible contrib adapter surfaces to participate in this runtime.

Execution surfaces that do not provide coherent heartbeat, cancel, and gc participation in this runtime are out of scope and MUST NOT be represented as live managed execution nodes under this contract.

### Observability

This live database MUST contain enough information to answer, for in-scope live nodes:

- which roots currently exist,
- which executions are currently live,
- which callers keep a given execution alive,
- whether a live execution is running, terminal, or cancel-requested,
- whether a live execution is past its graceful deadline.

This document does not require historical observability after rows are removed.

## Invariants

- Every live execution node id is `cache:<cache_key>`.
- Every live root node id is `index:<index-id>`.
- `index` nodes MUST NOT have non-`NULL` `adapter` or `uri`.
- `cache` nodes MUST have non-`NULL` `adapter` and `uri`.
- Duplicate caller edges MUST NOT exist.
- Caller-resolution precedence is:
  1. `DML_CACHE_KEY`
  2. adapter payload `parent_id`
  3. none
- A `cache:*` node with zero incoming edges is orphaned.
- After cancellation or sweep reaches a fixed point, no live orphaned `cache:*` node may remain.
- This database contains live objects only.

## Error Semantics

- SQLite lock contention:
  - transient
  - retryable
  - required runtime behavior: retry bounded write operations or fail the command or invocation with a clear runtime error
- graceful cancel failure:
  - transient or terminal depending on executor response
  - required runtime behavior: continue to forced cancellation when deadline expires
- forced cancel failure:
  - terminal for that sweep or cancel attempt
  - required runtime behavior: retain the live node row and surface the failure so a later `sweep` can retry
- cleanup failure:
  - terminal for that sweep or cancel attempt
  - required runtime behavior: retain the live node row and surface the failure so a later `sweep` can retry
- missing external live handle referenced by `metadata_json`:
  - not by itself an error if cleanup can complete successfully
  - required runtime behavior: treat cleanup as best effort and continue row removal when executor cleanup contract is satisfied

## Security Boundaries

This document defines no new external trust boundary.

`DML_CACHE_KEY` and adapter payload fields consumed under this contract are runtime-internal execution data for contrib-managed execution surfaces.

## Authority Handoffs

- Adapter-boundary payload and output schema are authoritative in [../adapter-execution-contract.md](../adapter-execution-contract.md).
- Contrib adapter and executor lifecycle callable semantics are authoritative in [runtime-contract.md](runtime-contract.md).
- Shared execution-state field semantics and metadata conventions are authoritative in [executor-state.md](executor-state.md).
- Structured status-report formatting is authoritative in [status.md](status.md).

## References

- [runtime-contract.md](runtime-contract.md)
- [executor-state.md](executor-state.md)
- [status.md](status.md)
- [../adapter-execution-contract.md](../adapter-execution-contract.md)
