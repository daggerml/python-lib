## Why

Execution-aware runtime code currently depends on a separate `execution_context` context manager and `ContextVar` pair to discover the current `execution_id`. That splits runtime identity across two mechanisms: resolved runtime config for normal session state, and ambient context for execution state. The split makes worker entrypoints leaky, hides execution-sensitive behavior behind process-local state, and forces lower layers to infer current execution identity indirectly.

We want execution identity to live in the same resolved runtime configuration model as the rest of the `Dml` session context, while keeping lower-level ops explicit about when they need that identity.

## What Changes

- Add `execution.id` to the shared internal runtime configuration model.
- Resolve `execution.id` only from explicit overrides and environment variables; it is not persisted in global or project config files.
- Update `Dml` runtime construction to accept `execution_id` as a root runtime override input.
- Remove `_internal.execution_context` and stop using `ContextVar` state to discover the current execution.
- Thread `execution_id` and `caller_execution_id` explicitly through runtime methods that publish runnable DAG results or record nested execution lineage.
- Make `start_fn` receive `caller_execution_id` explicitly, with the runtime boundary passing `config.execution.id` when present and falling back to the current root `index_id`.
- Make `commit` always finalize the committing execution/root as `lifecycle = "succeeded"`; committing an `Error` value records a successful execution whose DAG result is an error, while runtime `failed` remains reserved for execution failures that prevent successful DAG completion.
- Keep `cache_key` as execution-state data derived from the staged call, not as a runtime config field.

## Capabilities

### New Capabilities
<!-- None. -->

### Modified Capabilities
- `shared-internal-configuration`: the canonical runtime configuration model now includes ephemeral `execution.id` with explicit/env-only resolution semantics.
- `unified-dml-surface`: the shared `Dml` constructor and runtime methods accept execution-aware overrides without relying on ambient execution context.
- `runtime-execution-records`: runnable DAG publication and nested execution bookkeeping consume explicit execution identity instead of reading a process-local execution context, and `commit` success is distinguished from committed DAG error values.

## Impact

- Affected code: `_internal.config`, `_internal.dml_context`, `_internal.dml`, `_internal.ops.index`, contrib adapter/executor entrypoints, and execution-focused tests.
- Affected behavior: execution-aware worker sessions are identified by resolved runtime config and explicit runtime method args instead of `ContextVar` state, and `start_fn` stops discovering caller identity ambiently.
- Cleanup: `_internal.execution_context` and its exports/imports are removed.
