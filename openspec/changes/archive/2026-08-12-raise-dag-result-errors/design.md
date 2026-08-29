## Context

Committed DAGs are terminal with exactly one of a result-node ref or error ref. `Dag.result` reads the DAG description but currently only recognizes a result ref, so a failed DAG falls through to the message intended for a non-terminal DAG. The public DAG namespace already provides `get_error()` to hydrate and validate a stored error ref.

## Goals / Non-Goals

**Goals:**
- Make terminal failure visible through `Dag.result` by raising the persisted error.
- Preserve successful-result and non-terminal behavior.
- Use the existing public error hydration path and error type.
- Keep public authoring and error guidance aligned with the accessor contract.

**Non-Goals:**
- Change failed-node behavior or `NodeError` context handling.
- Change persisted DAG or error representations.
- Change how callers inspect errors through low-level DAG APIs.

## Decisions

### Raise the hydrated base Error before checking for a result ref

`Dag.result` SHALL inspect the described `error` field and, when it is an error ref, call `dml.dag.get_error()` and raise the returned `Error`. This matches the terminal-state model and lets callers receive the persisted message, origin, type, and stack.

The accessor will continue to return a node only for a valid result ref. If neither terminal field is present, it will retain the existing repository error. Checking the error first clearly prioritizes the failed terminal state and remains compatible with the storage invariant that result and error cannot coexist.

Alternative considered: raise a new `DmlRepoError` containing the stored error message. Rejected because it loses the persisted error's type, origin, and stack and diverges from existing stored-error propagation.

Alternative considered: raise `NodeError`. Rejected because a DAG-level terminal error has no failed node ref or function-DAG node context to enrich.

## Risks / Trade-offs

- [Mocked API tests may not configure `get_error()`] → Add an explicit contract fixture/configuration and assertion for the hydration call.
- [A mocked contract test may miss persistence-boundary behavior] → Add an API integration test that loads a DAG committed with an error and accesses its result.
- [Malformed descriptions could contain both terminal refs] → The persistent type validator already rejects this state; the accessor follows the error branch if externally mocked malformed data reaches it.
