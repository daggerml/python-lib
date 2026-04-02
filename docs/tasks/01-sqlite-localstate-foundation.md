# Task 01 - SQLite LocalState Foundation

## Objective

Replace flat-file `LocalState` storage with a SQLite-backed local runtime state layer that owns live `nodes` and `edges` tables while preserving the existing state-record API used by local executors.

## Scope

In scope:

- `src/daggerml/contrib/executor_state.py`
- any new SQLite helper module under `src/daggerml/contrib/`
- local runtime DB initialization and locking for `LocalState`
- tests for SQLite-backed `LocalState`

Out of scope:

- adapter caller propagation
- executor `cancel(state)` implementations
- CLI commands

## Affected Interfaces And Contracts

- Public-ish contrib runtime boundary in code:
  - `class LocalState(StateBase)` remains the local executor entrypoint.
- Existing state-view methods must remain callable:
  - `get() -> StateRecord | None`
  - `put_if_absent(state: StateRecord) -> bool`
  - `update(state: StateRecord) -> None`
  - `delete() -> None`
  - `lock() -> ContextManager[LocalState | None]`
- Constructor may grow local-runtime graph parameters, but must keep `cache_key: str` as the first required argument.

Exact state DB schema to create:

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

create table edges (
  src_id text not null,
  dst_id text not null,
  primary key (src_id, dst_id),
  foreign key (src_id) references nodes(id) on delete cascade,
  foreign key (dst_id) references nodes(id) on delete cascade
);

create index edges_dst_idx on edges(dst_id);
```

Implementation requirements:

- SQLite must use WAL mode.
- SQLite must set a bounded busy timeout.
- `LocalState` locking must become SQLite-backed single-writer coordination for one `cache:<cache_key>` node mutation path.
- `StateRecord` shape and validation rules remain unchanged in this task.
- `LocalState` must store and read the `StateRecord` view from the `nodes` row for `cache:<cache_key>`.

## Required Tests Or Validation

- Add unit tests covering:
  - DB initialization creates `nodes` and `edges`
  - `put_if_absent()` creates exactly one `cache:*` node
  - `update()` mutates status, error, heartbeat, and metadata view correctly
  - `delete()` removes the `cache:*` node
  - lock contention returns `None` for a second lock holder within timeout bounds
- Expected outcomes:
  - no flat files are created
  - repeated `put_if_absent()` for the same `cache_key` is idempotent
  - existing `StateBase` helpers still operate through `LocalState`

## Commit Expectation

Create one commit containing only the SQLite `LocalState` foundation and its tests.
