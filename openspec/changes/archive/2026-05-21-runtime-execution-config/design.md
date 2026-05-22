## Context

The repo already has one shared runtime configuration model for project home, remote settings, user identity, and related session state. Execution identity sits outside that model in `_internal.execution_context`, which adapters and executors must enter before lower runtime layers can publish runnable DAGs or attribute nested executions.

That arrangement has three costs:

- worker entrypoints must remember to establish ambient execution context
- lower layers depend on hidden process-local state for execution-sensitive behavior
- execution identity is modeled differently from all other runtime session inputs

## Goals / Non-Goals

**Goals:**
- move current execution identity into the shared runtime configuration model as `execution.id`
- remove `_internal.execution_context`
- keep execution-sensitive ops explicit by threading `execution_id` / `caller_execution_id` through the runtime boundary
- keep `cache_key` derived from staged call state rather than promoting it to runtime config

**Non-Goals:**
- changing the meaning of `cache_key`
- changing adapter envelope fields
- redesigning execution-record storage layout
- collapsing top-level index-root execution records and ordinary execution attempts into one identity type

## Decisions

### Decision: `execution.id` is runtime config, not file-backed config

`execution.id` joins the canonical resolved config model, but only as an ephemeral runtime field. It resolves from:

- explicit constructor/runtime overrides
- environment variables

It does not load from project or global config files and is not persisted by config mutation workflows.

Rationale:
- it belongs to the runtime session model
- it should be available anywhere resolved runtime config is already available
- it is not user-managed repository configuration

### Decision: Lower layers receive explicit execution identity

The runtime boundary reads resolved `execution.id` and passes it to lower-level ops where needed.

Key call paths:

- `runtime.commit(..., execution_id=...)` for runnable DAG cache publication
- `runtime.start_fn(..., caller_execution_id=...)` for nested execution lineage

For `start_fn`, the runtime boundary resolves caller identity as:

- `config.execution.id`, when running inside an execution-aware worker
- otherwise the current root `index_id`

`IndexOps.start_fn` therefore does not discover caller identity on its own.

Rationale:
- keeps ops behavior explicit and testable
- avoids replacing one hidden ambient mechanism with another

### Decision: `commit` always records execution success

`IndexOps.commit` will always finalize the committing execution record as `lifecycle = "succeeded"`.

This is true even when the committed DAG result is an `Error` object. A committed `Error` means the execution successfully produced a DAG whose terminal result is an error value. It is not a runtime execution failure.

Runtime `failed` remains reserved for cases where execution did not successfully produce a committed DAG result, such as malformed adapter requests or other execution-path failures that prevent successful DAG completion.

The implementation should include a code comment at the `commit` lifecycle update site documenting this distinction.

Rationale:
- execution lifecycle describes whether the runtime successfully produced and finalized a DAG result
- DAG content may itself represent an application-level error result
- conflating committed `Error` values with runtime execution failure would blur a key contract boundary

### Decision: Caller lineage updates no longer require caller cache key threading

When a caller execution spawns a callee execution, the runtime records the edge and then updates the caller's `execution_record.spawned_execution_ids` by reading and CAS-updating the caller record addressed by `caller_execution_id`.

This applies both to normal execution callers and top-level root callers, since root index creation already establishes `exec/state/<index_id>.json`.

Rationale:
- caller execution records already store their own `cache_key`
- the lineage update only needs caller execution identity
- this removes unnecessary argument threading

## Migration Plan

1. Extend resolved runtime config with ephemeral `execution.id` support.
2. Update `Dml` runtime construction and runtime namespace methods to accept/pass explicit execution identity.
3. Refactor `IndexOps.commit` and `IndexOps.start_fn` to stop reading ambient execution context and to require caller identity from the runtime boundary.
4. Update `IndexOps.commit` so it always marks the committing execution/root `succeeded`, with a code comment documenting why committed `Error` values are still successful executions.
5. Remove `_internal.execution_context` and update contrib entrypoints/tests.
6. Update specs and execution-focused tests for the new runtime identity boundary.

## Risks / Trade-offs

- [Execution-aware call sites may fail to pass identity] -> Mitigation: centralize threading at the `Dml.runtime` boundary and cover worker entrypoints with tests.
- [Resolved config may appear to blur persistent and ephemeral fields] -> Mitigation: specify that `execution.id` is explicit/env-only and never file-backed.
- [Caller execution record may be missing during nested lineage update] -> Mitigation: require the runtime boundary to pass `config.execution.id or index_id` and treat any missing `exec/state/<caller_execution_id>.json` as a runtime invariant failure.
- [Committed DAG error values may be mistaken for runtime execution failure] -> Mitigation: specify that `commit` always records `succeeded`, require a code comment at the lifecycle update site, and cover the distinction in specs/tests.

## Open Questions

- whether user-facing `config show` should omit `execution.id` even if the internal resolved config model includes it
