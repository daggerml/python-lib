## Why

Runtime cancellation is currently split across `Dml.runtime.cancel` and `IndexOps.cancel`, relies on retry-loop ownership transfer, and uses detached cancellation semantics that no longer match the intended model.

## What Changes

- Make `Dml.runtime.cancel` a thin public entrypoint and move the real cancellation engine into `IndexOps.cancel`.
- Treat indexes and executions the same for cancellation; index roots add pointer move/delete behavior.
- Keep each execution responsible for its own state; one `cancel(this_exec)` call mutates `this_exec` directly, and child state changes happen only inside nested `cancel(child)` runtime calls.
- Use only `cancel-pending` and `cancelled` lifecycle values for cancellation.
- Clear `active/<cache_key>` under the cache-key lock during cancellation.
- Process direct child edges concurrently with a thread pool.
- Require adapters to participate synchronously: for each direct child execution, the adapter chain responsible for that child owns both execution and cancellation of that child job, may delegate to `Dml.runtime.cancel(child)` at most once, then tears down its own infrastructure and returns. By convention, the last adapter in the chain usually owns both kickoff and cancellation, but that is not a required contract.

## Capabilities

### Modified Capabilities

- `execution-admin-controls`
- `runtime-execution-records`
- `executor-cancellation`
- `execution-call-edges`

## Impact

- Affected code: `src/daggerml/_internal/dml.py`, `src/daggerml/_internal/ops/index.py`, `src/daggerml/_internal/exec_state.py`, contrib executors, and cancellation tests.
- Affected contracts: lifecycle values, cancellation requester semantics, adapter cancel flow, and edge-removal behavior.
