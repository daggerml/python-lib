## MODIFIED Requirements

### Requirement: Shared `Dml` exposes runtime cancel with explicit mode selection
The shared `Dml` runtime namespace SHALL expose cancellation as `dml.runtime.cancel(index_or_execution, mode="full")`. `mode` SHALL accept `"full"` and `"drive"`.

- `mode = "full"` SHALL run the full root-facing cancellation workflow.
- `mode = "drive"` SHALL run only the cancellation driver needed by an already-canceling execution.

#### Scenario: Runtime namespace exposes full cancellation by default
- **WHEN** a caller invokes `dml.runtime.cancel(idx1)` without an explicit mode
- **THEN** the runtime namespace SHALL use `mode = "full"`

#### Scenario: Runtime namespace exposes drive mode for internal cancellation progress
- **WHEN** a caller invokes `dml.runtime.cancel(e1, mode="drive")`
- **THEN** the runtime namespace SHALL expose the driver-only cancellation behavior for that execution
