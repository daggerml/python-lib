# Task 04 - Cancel Command And Orphan Propagation

## Objective

Implement `dml contrib cancel <index-id>` so it removes root ownership edges and runs orphan-driven cancellation until the execution graph reaches a fixed point.

## Scope

In scope:

- `dml contrib cancel <index-id>` CLI surface
- local runtime graph traversal and mutation helpers
- orphan detection
- graceful cancel request marking
- forced-cancel deadline handling during cancel processing
- tests for shared-subgraph cancellation behavior

Out of scope:

- general maintenance sweep command
- historical retention

## Affected Interfaces And Contracts

- New CLI contract:
  - `dml contrib cancel <index-id>`
  - input: one non-empty `index-id`
  - behavior: remove outgoing edges from `index:<index-id>` and apply orphan propagation
  - error: report missing or invalid runtime state cleanly without destructive fallback
- Runtime graph behavior:
  - orphan = `cache:*` node with zero incoming edges
  - for each orphan:
    - set `cancel_requested_ts` if not already set
    - request graceful `cancel(state)`
    - if `cancel_requested_ts + 2 * HEARTBEAT_STALENESS < now` and node is still non-terminal, force cancel
    - once terminal, run `gc(state)`, remove outgoing edges, remove node row

Invariants to preserve:

- shared descendants with another incoming caller edge must remain live
- repeated `cancel <index-id>` is idempotent
- no live orphaned `cache:*` node may remain after propagation settles unless executor cancellation or cleanup returned a surfaced failure

## Required Tests Or Validation

- Add tests covering:
  - canceling a root removes only that root's edges
  - a shared child execution survives when another root still reaches it
  - an unshared child execution is canceled and cleaned up
  - repeated cancel is idempotent
  - force-cancel path triggers after deadline expiration
- Expected outcomes:
  - graph shrinks only where ownership was removed
  - terminal cleaned nodes disappear from live tables

## Commit Expectation

Create one commit containing the cancel CLI, orphan propagation logic, and tests.
