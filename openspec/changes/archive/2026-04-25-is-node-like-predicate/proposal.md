## Why

Executor validation logic (e.g., `SshExecutor._validate_kw`) currently checks against `DelayedActionCodec` directly, which is an internal codec type rather than the user-facing `Delayed*` types (`DelayedRef`, `DelayedLoad`, `DelayedRunnable`). There is no shared predicate for "is this value a node or a delayed node-like value", so each executor duplicates the pattern inline and may get it wrong or inconsistently.

## What Changes

- Introduce a shared `is_node_like(x)` predicate function that returns `True` if `x` is an instance of `Node` or any of the `Delayed*` types (`DelayedRef`, `DelayedLoad`, `DelayedRunnable`).
- Update `SshExecutor._validate_kw` (and any other executor validation that checks for `Node`-or-deferred values) to use `is_node_like` instead of inline `isinstance` checks.

## Capabilities

### New Capabilities
- `is-node-like-predicate`: A shared predicate `is_node_like(x)` in the contrib API module that identifies values acceptable as node-like (i.e., `Node` or any `Delayed*` type), for use in executor validation and elsewhere.

### Modified Capabilities

## Impact

- `src/daggerml/contrib/api.py` — add `is_node_like` function
- `src/daggerml/contrib/executors/ssh.py` — update `_validate_kw` to use `is_node_like`
- Other executors (`docker.py`, `script.py`, `batch.py`) may also benefit but are out of scope for this change
