# Task 03 - Local Executor SQLite Migration

## Objective

Migrate every local executor that creates state so it persists live handles through SQLite-backed `LocalState` and implements runtime-owned cancellation hooks.

## Scope

In scope:

- local executors that create state under `src/daggerml/contrib/executors/`
- executor metadata writes into `LocalState`
- `cancel(state)` implementation for each in-scope local executor
- tests for executor handle persistence and idempotent cancel or gc behavior

Out of scope:

- CLI cancel or sweep orchestration
- lambda or other remote execution surfaces that do not participate in the local runtime DB

## Affected Interfaces And Contracts

- In-scope executors are all local executors that create state.
- Each in-scope executor must support:
  - `start(*, runnable, argv_ptr, cache_key, remote, state=None)`
  - `poll(*, state=None)`
  - `cancel(*, state=None)`
  - `gc(*, state=None)`
- `cancel(*, state=None)` requirements:
  - best-effort graceful stop first
  - safe to call more than once
  - may escalate when runtime indicates force-cancel path through state metadata or helper API
- `gc(*, state=None)` requirements:
  - idempotent
  - removes executor-owned live residue

Metadata contract:

- executor-owned live handles must remain under `StateRecord.metadata[<executor_id>]`
- enough metadata must be written to support later `poll`, `cancel`, and `gc`
- no executor may continue writing private flat-file state outside the SQLite runtime DB for state it owns under this contract

## Required Tests Or Validation

- Add or update executor tests covering each in-scope local executor for:
  - writing live handle metadata into `LocalState`
  - `poll()` reading that metadata back correctly
  - `cancel()` being safe to call twice
  - `gc()` being safe to call twice
- Expected outcomes:
  - local stateful executors use SQLite-backed state only
  - executor cleanup still works after terminal completion and after cancellation

## Commit Expectation

Create one commit containing the local executor migrations and the related tests only.
