## Why

The dashboard currently reaches into `daggerml._core` submodules and private
`Dml` state, coupling a browser-facing inspection service to repository storage
internals. It also reads executor-local logs and probes executor resources,
which expands the dashboard's trust and observability boundary beyond the
public DaggerML interface.

## What Changes

- Refactor dashboard server code to use only the existing public APIs exported
  by `daggerml._core`, `daggerml`, and `daggerml.contrib`; it will not import a
  `daggerml._core` submodule or access private `Dml` fields.
- Replace direct commit, ref, configuration, and execution-state access with
  public `Dml` methods and dashboard-owned bounded projections.
- Restrict dashboard log inspection and streaming to canonical CloudWatch
  stdout and stderr streams derived from trusted persisted cache identity.
- Remove local executor log reads and executor-specific PID, Docker, Batch, and
  CloudFormation probes. Display runnable configuration and persisted,
  redacted launch state as bounded JSON instead.
- Update dashboard API contracts, frontend labels, tests, and architecture and
  security documentation to reflect the narrowed boundary.

## Capabilities

### New Capabilities
- `dashboard-public-api-boundary`: Public-API-only dashboard repository and
  runtime inspection, CloudWatch-only logs, and safe runnable evidence.

### Modified Capabilities
- `dashboard-value-runnable-inspection`: Replace executor-specific resource and
  local-log inspection requirements with bounded public launch-state and
  CloudWatch-only log behavior.

## Impact

- Affects `src/daggerml/dashboard/**`, dashboard tests, packaged frontend
  assets, and dashboard architecture and security documentation.
- Uses existing public `Dml` namespace operations; does not add or change the
  public DaggerML API or persisted schemas.
- Removes dashboard access to local executor logs and live executor-resource
  status, so those panels may report unavailable rather than probing locally.
