## Why

Failed function-call nodes are persisted with an error ref, but normal node materialization re-raises the error before callers can inspect the failed node's provenance. Core resolution also uses stored errors as control flow, making inspection and execution handling less explicit.

## What Changes

- Return either a datum ref or an error ref from core node resolution without loading stored errors.
- Allow low-level DAG inspection to return a hydrated stored `Error`, and add a public `dml.dag.get_error(error_ref)` query.
- Raise a transient API `NodeError` when high-level node construction or value materialization reaches a stored error; it carries the failed node ref and can return its failed function-DAG context.
- Keep `start_fn()` returning only `Ref | None`; an error-resolving argument raises before a call is created or dispatched.
- Canonicalize every persisted `Error` subclass to the base `Error` at the single transaction storage boundary.
- Keep the scope minimal: errors remain unsupported literal values and no new failed-node wrapper, result type, or error-insertion path is introduced.

## Capabilities

### New Capabilities
- `node-error-inspection`: Inspect persisted node failures by ref while preserving fail-fast high-level node access and execution boundaries.

### Modified Capabilities

None.

## Impact

- Affects core node resolution and DAG queries in `src/daggerml/_core/types.py` and `src/daggerml/_core/dml.py`.
- Affects high-level node materialization in `src/daggerml/api.py`.
- Adds a generated `dml dag get-error` command through the existing CLI reflection mechanism; no CLI framework changes are expected.
- Requires focused core, API, and CLI contract coverage plus user-facing error and DAG inspection documentation.
