## 1. Execution Record Schema

- [x] 1.1 Extend `ExecutionRecord` in `src/daggerml/_core/exec_state.py` with `created_at` and `child_execution_ids`.
- [x] 1.2 Update all execution-record creation paths so root and nested execution records initialize `created_at`, `updated_at`, `spawned_execution_ids`, and `child_execution_ids` consistently.
- [x] 1.3 Rename and update the spawned-child completion helper so terminal direct children move from `spawned_execution_ids` into `child_execution_ids` while preserving dedupe and disjointness.

## 2. Graph Extraction

- [x] 2.1 Add an execution-state graph query in `src/daggerml/_core/exec_state.py` that accepts root execution-id strings and recursively traverses `spawned_execution_ids` and `child_execution_ids`.
- [x] 2.2 Shape the graph response as `{roots, nodes}` with node payload fields `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `created_at`, `cancel_requested_by`, `children`, and `spawned`.
- [x] 2.3 Make the graph query strict for missing root execution records and ensure unrelated executions are excluded from the returned closure.

## 3. Public Runtime Surface

- [x] 3.1 Add `Dml.runtime.describe_graph(*roots: Ref | str)` in `src/daggerml/_core/dml.py`.
- [x] 3.2 Normalize explicit `Ref | str` roots to execution-id strings and default empty input to the ids of `dml.runtime.list()` entries before delegating to execution-state graph extraction.

## 4. Verification

- [x] 4.1 Add or update execution-state tests covering record initialization, spawned-to-child transitions, and descendant-only graph traversal.
- [x] 4.2 Add or update shared DML/runtime tests covering `describe_graph()` with explicit roots and with empty input defaulting to open local indexes.
- [x] 4.3 Run the relevant contract and integration tests for execution coordination and runtime surface behavior.
