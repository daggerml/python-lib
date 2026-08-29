## Why

Cancellation is currently keyed by execution id and stops at recording `cancel-requested`, which does not match user intent or actually tear down contrib-managed work. We need index-rooted cancellation that freezes the index, walks the rooted live call graph, and lets executors perform their own cancel-time cleanup.

## What Changes

- Add `Dml.runtime.cancel(index_id)` as the caller-facing cancellation entrypoint.
- Treat each `index_id` as a synthetic execution root in `dml/exec/state/*` and `dml/exec/edges/*` so index lineage uses the same S3 graph model as runtime executions.
- Change cancellation planning to start from an index root instead of user-supplied execution ids.
- Atomically move the target index to `indexes/.cancelled/<id>.json` under lock so the index is frozen during cancellation and cannot be modified further.
- Walk the rooted active execution graph for that cancelled index and mark eligible executions `cancel-requested` under per-execution locks after rechecking terminal state and live-caller ownership.
- Update contrib executors to treat `cancel-requested` as an update step and perform executor-owned teardown of external resources.
- Let executors that normally call `runnable.sub` on update continue doing so during cancellation; let executors that do not call `runnable.sub` cancel their own external jobs directly.
- Complete the bounded cancellation sweep by deleting the temporary `indexes/.cancelled/<id>.json` marker after the algorithm runs.

## Capabilities

### New Capabilities
- `executor-cancellation`: executor-side handling of `cancel-requested`, including update-time sub-dispatch rules and teardown of external resources for script, docker, batch, cfn, and ssh flows.

### Modified Capabilities
- `execution-admin-controls`: change cancellation from execution-id-rooted planning to index-rooted planning with per-execution lock/recheck semantics and bounded sweep completion.
- `runtime-execution-records`: extend execution-state storage so index ids can be stored and traversed as synthetic execution roots.
- `execution-call-edges`: allow rooted lineage edges whose caller id is an index id stored in the same canonical edge namespace.
- `unified-dml-surface`: add `runtime.cancel` to the shared `Dml` runtime namespace.

## Impact

- Affected code: `src/daggerml/_internal/dml.py`, runtime/index and remote cancellation planning, and contrib executors/adapters.
- Affected systems: execution graph traversal, index lifecycle, remote execution state and edge storage, and external executor backends such as Batch, Docker, SSH, supervisor-managed scripts, and CloudFormation.
- Caller impact: cancellation moves from execution-id-oriented internals to an index-oriented runtime API that better matches user intent.
