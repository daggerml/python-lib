## 1. Runtime Config Model

- [x] 1.1 Add ephemeral `execution.id` to the canonical resolved internal config model.
- [x] 1.2 Define `execution.id` resolution as `explicit > environment > null` with no project/global config-file source.
- [x] 1.3 Update runtime-context helpers and constructor wiring so execution-aware workers can instantiate `Dml(..., execution_id=...)`.

## 2. Runtime Identity Threading

- [x] 2.1 Update runtime namespace methods to pass explicit `execution_id` into execution-sensitive `IndexOps` paths and to pass `caller_execution_id = config.execution.id or index_id` for `start_fn`.
- [x] 2.2 Refactor `IndexOps.commit` to use explicit `execution_id`, always CAS-update the committing execution/root record to `succeeded`, and publish cache only when the committed DAG has `argv`.
- [x] 2.3 Refactor `IndexOps.start_fn` to stop discovering caller identity ambiently and to CAS-update the caller execution record by `caller_execution_id` only.
- [x] 2.4 Add a code comment at the `commit` lifecycle update site explaining that committed `Error` values are successful executions and runtime `failed` is reserved for execution-path failures.

## 3. Context Removal And Validation

- [x] 3.1 Remove `_internal.execution_context` and its exports/imports.
- [x] 3.2 Update contrib adapter/executor entrypoints and execution test assets to stop establishing ambient execution context.
- [x] 3.3 Update/add tests and spec expectations for resolved runtime execution identity, explicit execution-aware runtime plumbing, and the distinction between committed DAG errors and runtime `failed` executions.
