# Task 05 - Sweep Command And Runtime Consistency

## Objective

Implement `dml contrib sweep` as the live-runtime maintenance pass that completes pending cancellation, cleans terminal nodes, and removes inconsistent live rows that can only be resolved by cancellation or deletion.

## Scope

In scope:

- `dml contrib sweep` CLI surface
- runtime sweep orchestration over SQLite `nodes` and `edges`
- stale-heartbeat handling for cancel-requested executions
- cleanup retry behavior for nodes left behind by earlier failures
- end-to-end validation of the staged SQLite runtime migration

Out of scope:

- history retention
- remote runtime repair

## Affected Interfaces And Contracts

- New CLI contract:
  - `dml contrib sweep`
  - inputs: none
  - behavior: inspect live graph, apply orphan propagation, enforce forced-cancel deadlines, run cleanup, remove fully cleaned live rows
- Runtime behavior:
  - `sweep` must not invent historical state
  - `sweep` may only repair by canceling live executions, running cleanup, and deleting live rows or edges that are no longer valid
  - cleanup and forced-cancel failures must leave the affected live rows present for a later retry

Validation boundary:

- full local runtime flow must still satisfy existing adapter output contracts
- local executors that create state must all operate through SQLite-backed `LocalState`

## Required Tests Or Validation

- Add tests covering:
  - `sweep` cancels orphaned live executions left behind without an explicit cancel command
  - `sweep` force-kills cancel-requested executions past deadline
  - `sweep` retries cleanup on a later run after an earlier failure
  - `sweep` removes empty `index:*` roots with no remaining purpose
- Run the targeted contrib test suite affected by Tasks 01-05.
- Expected outcomes:
  - no live orphaned `cache:*` nodes remain after a successful sweep
  - no executor-created local state remains outside SQLite
  - all updated tests pass

## Commit Expectation

Create one commit containing the sweep CLI, runtime consistency logic, and final validation updates.
