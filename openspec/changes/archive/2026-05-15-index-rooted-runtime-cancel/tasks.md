## 1. Runtime Cancel Entry Point

- [x] 1.1 Add `Dml.runtime.cancel(index_id)` and return a JSON-ready cancellation summary.
- [x] 1.2 Implement index locking and atomic move from `indexes/<id>.json` to `indexes/.cancelled/<id>.json`.
- [x] 1.3 Ensure cancelled indexes are treated as frozen and cannot be mutated through normal runtime index workflows.
- [x] 1.4 Persist `exec/state/<index_id>.json` for live indexes and keep their rooted `dependencies` updated as executions are launched.

## 2. Index-Rooted Cancellation Planning

- [x] 2.1 Record rooted lineage edges in `exec/edges/<callee>/<caller>.json` when an index launches an execution.
- [x] 2.2 Resolve the rooted active execution set from `{index_id}` instead of from user-supplied execution ids.
- [x] 2.3 Add per-execution locking around terminal-state recheck, dependency expansion, active-caller counting, and `cancel-requested` state updates.
- [x] 2.4 Invoke adapter update paths with `execution_status="cancel-requested"`, persist terminal `cancelled` when returned, and delete the temporary cancelled-index marker when the bounded sweep completes.

## 3. Executor Cancellation Behavior

- [x] 3.1 Update shared executor handling so `cancel-requested` is treated as an update step and update-dispatch executors continue sub-dispatch during cancellation.
- [x] 3.2 Implement script and docker cancel teardown for supervisor/process groups, containers, and temporary images or workdirs.
- [x] 3.3 Implement batch and cfn cancel teardown so Batch jobs are canceled and deregistered and CloudFormation starts rollback quickly with stack context.
- [x] 3.4 Confirm ssh cancel updates pass through the nested adapter result without adding extra remote wrapper state.
- [x] 3.5 Update adapter/executor validation so `cancelled` is accepted as a terminal cancel result.

## 4. Verification

- [x] 4.1 Add or update contract tests for `runtime.cancel`, cancelled-index freezing, and rooted cancellation planning.
- [x] 4.2 Add or update contrib executor tests covering cancel-update behavior for ssh, batch, docker, script, and cfn.
- [x] 4.3 Run the relevant targeted test suites for internal runtime cancellation and contrib executor cancellation.
