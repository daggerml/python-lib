## Why

The current runtime cancellation model mixes launch-state ownership, lifecycle ownership, and cancellation orchestration into one mutable execution object. That makes cancellation chunky, requires the cancelling runtime to synchronously drive adapter cancellation with permissions it may not have, and blurs which runtime is allowed to mutate which execution fields.

## What Changes

- Split the current execution object into two durable records: caller-owned `launch_state` and runtime-owned `execution_record`.
- Redefine cancellation as an out-of-band control-plane workflow that removes current-execution ownership, marks cancellation intent via CAS, and delegates final shutdown handling to adapters/executors.
- Rename lifecycle fields and statuses to make the weaker cancellation guarantee explicit, including replacing `cancelled` with a detached-state name that does not imply backend process termination.
- Preserve two distinct graph structures: caller-owned live caller edges for invalidation and orphan detection, and runtime-owned spawned execution lists for cancellation traversal.
- Update `Dml.runtime.cancel(index_id)` semantics to operate without an active caller `execution_id`, using `config.user` as the cancellation requester when invoked directly by a user.
- Document accepted best-effort cancellation limits, including the case where descendants behind already-terminal intermediates may not be cancelled.

## Capabilities

### New Capabilities
<!-- None. -->

### Modified Capabilities
- `execution-state`: redefine the S3 execution coordination contract around caller-owned `launch_state`, cache-key locking, and active-pointer removal during cancellation.
- `runtime-execution-records`: replace the monolithic execution object with `launch_state` and `execution_record`, rename lifecycle fields/statuses, and update cancellation CAS semantics.
- `executor-cancellation`: align executor cancellation with out-of-band `cancellation-pending` updates and detached completion semantics.
- `execution-call-edges`: clarify that live caller edges are caller-owned and distinct from runtime-owned cancellation dependencies.
- `unified-dml-surface`: update `dml.runtime.cancel(index_id)` requirements for direct user-triggered cancellation with no active execution context.

## Impact

- Affected code: `src/daggerml/_internal/ops/index.py`, `src/daggerml/_internal/exec_state.py`, `src/daggerml/_internal/dml.py`, executor implementations, and runtime contract tests.
- Affected APIs/contracts: runtime execution persistence, adapter envelope lifecycle fields, executor cancellation behavior, and `dml.runtime.cancel(index_id)` semantics.
- Affected systems: S3-backed execution coordination, invalidation lineage, adapter/executor cancellation flow, and contract/spec documentation.
