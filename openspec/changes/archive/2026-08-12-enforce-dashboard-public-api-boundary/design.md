## Context

The dashboard currently combines public `Dml` calls with direct imports from
core submodules, direct access to `Dml._db`, executor-local log reads, and live
executor probes. The current `Dml` facade already exposes the required
repository, ref, runtime, execution-record, launch-state, and lineage reads.
See proposal.md and the delta specs for the product and boundary requirements.

## Goals / Non-Goals

**Goals:**

- Make the dashboard depend solely on public DaggerML facades and public `Dml`
  namespace operations.
- Keep dashboard history, ref comparison, pagination, redaction, and URL
  projections bounded and dashboard-owned.
- Make CloudWatch the sole dashboard log backend.
- Present persisted runnable and launch-state evidence without interrogating
  executor environments.

**Non-Goals:**

- Add, rename, or expand public `Dml` APIs.
- Fetch or materialize remote objects while reading dashboard views.
- Preserve executor-local log or live-resource details behind a compatibility
  fallback.
- Change execution lifecycle authority, persisted schemas, or cancellation.

## Decisions

### Compose public repository projections

Replace direct `CommitOps`, `Head`, `Config`, and private database access with
public calls. `Dml.from_config_vars(...).config.show()` resolves dashboard
configuration; `show()` supplies commit descriptions and parent links;
`branch.list()` and `tag.list()` supply source-specific ref tips;
`branch.get_upstream()` and `status()` supply upstream/sync facts; and runtime
and DAG namespaces supply live and persisted graph reads.

The read model will retain bounded breadth-first parent walks, multi-tip
comparison, pagination, and route validation as dashboard composition. This
preserves the current response shape while moving storage semantics behind the
facade.

Alternative considered: add a dashboard-specific core read-model API. Rejected
because existing public operations cover the required reads and a new API would
expand the public contract unnecessarily.

### Use public values and structural serialization

Dashboard modules will import public DaggerML values from the permitted facades.
Serialization will recognize explicit `Ref`, `Uri`, `Runnable`, and `Error`
values, dataclasses, mappings, and sequences; it will remove reliance on the
internal `DmlBase` class.

Alternative considered: retain a generic internal base-class conversion.
Rejected because it violates the boundary and is unnecessary for dashboard
response projections.

### CloudWatch is the sole log backend

Log endpoints and SSE streams will derive a cache key only from trusted selected
execution or function-DAG state, then read `/run/{cache_key}/{stream}` in the
CloudWatch `dml` group. Missing cache identity, client configuration, stream,
or access produces the established bounded availability response. The local-log
reader and all fallback call paths are removed.

Alternative considered: keep local logs as an offline fallback. Rejected by the
required observability boundary: local paths expose host files and make log
behavior depend on executor placement.

### Present persisted evidence without probes

The runnable inspector will retain safe static configuration and add bounded,
redacted raw launch-state JSON from `runtime.read_launch_state()`. It will stop
calling Docker, Batch, CloudFormation, PID, and analogous resource probes. API
and frontend terminology will use Runnable or launch state rather than Resources
where the old panel meant live executor status.

Alternative considered: retain probes but isolate them behind optional clients.
Rejected because optional execution still violates the policy and yields
environment-dependent results.

## Risks / Trade-offs

- [Some previously visible logs are no longer available] -> Return explicit
  CloudWatch-unavailable diagnostics with no local fallback.
- [Live executor status disappears] -> Preserve persisted runnable and launch
  state as clearly non-authoritative evidence.
- [Public-call composition increases dashboard request work] -> Retain current
  caps, opaque cursors, and bounded parent/ref traversal.
- [Ref comparison no longer uses core graph helpers directly] -> Compare only
  bounded, publicly inspected reachable commit sets and report unknown where
  the limit or unavailable tips prevent a definite relation.

## Migration Plan

1. Replace dashboard imports and private `Dml` access with permitted facade
   imports and public calls, retaining bounded response projections.
2. Remove local log reading and live-resource probe code; route log reads and
   streams exclusively through trusted CloudWatch identities.
3. Update inspector responses, frontend labels, contracts, and documentation.
4. Verify no dashboard source imports a core submodule or references private
   `Dml` state, then run dashboard and API contract tests.

Rollback restores the prior dashboard package release. No repository data or
schema migration is required.
