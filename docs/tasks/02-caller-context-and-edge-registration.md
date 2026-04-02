# Task 02 - Caller Context And Edge Registration

## Objective

Teach the local contrib runtime to resolve caller identity and register live caller edges in the SQLite execution graph for top-level and nested invocations.

## Scope

In scope:

- local contrib adapter payload parsing and runtime dispatch
- `LocalState` graph-edge helpers
- environment propagation needed for nested local calls
- tests for caller resolution and edge creation

Out of scope:

- executor cancel implementations
- CLI cancel or sweep commands
- remote adapter participation beyond preserving current behavior

## Affected Interfaces And Contracts

- Caller resolution contract is unchanged from `docs/contrib/execution-graph.md`:
  1. `cache:<DML_CACHE_KEY>` when `DML_CACHE_KEY` is set
  2. else `index:<parent_id>` when adapter payload includes `parent_id`
  3. else no caller edge
- `LocalState` must expose graph helpers sufficient to support:
  - upserting caller node
  - upserting callee node
  - inserting idempotent edge `src_id -> dst_id`
  - removing a caller edge when the caller is no longer waiting

Exact boundary expectations:

- top-level local adapter invocation with `parent_id=<id>` and no `DML_CACHE_KEY` creates:
  - `nodes.id = 'index:<id>'`
  - `edges.src_id = 'index:<id>'`
  - `edges.dst_id = 'cache:<cache_key>'`
- nested local invocation with `DML_CACHE_KEY=<parent_cache_key>` creates:
  - `edges.src_id = 'cache:<parent_cache_key>'`
  - `edges.dst_id = 'cache:<child_cache_key>'`

Implementation invariants:

- edge insertion must be idempotent
- no mixed raw ids; persisted ids must include `index:` or `cache:` prefixes
- `DML_CACHE_KEY` must be propagated through local executor-launched child processes that can invoke nested funks

## Required Tests Or Validation

- Add tests covering:
  - top-level local call creates `index:* -> cache:*`
  - nested local call creates `cache:* -> cache:*`
  - repeated polls do not create duplicate edges
  - edge release removes caller ownership when the caller is done waiting
- Expected outcomes:
  - caller precedence follows env var first, payload second
  - graph ids are always prefixed correctly

## Commit Expectation

Create one commit containing only caller-resolution, env propagation, edge registration, and tests.
