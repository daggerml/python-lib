## Why

Users can coordinate and cancel runtime executions today, but there is no direct shared-`Dml` method for reading the stored execution record that represents an execution's current state. That makes execution-state inspection harder than it needs to be for callers who already know an execution id or have a runtime index ref.

## What Changes

- Add a shared-`Dml` runtime inspection method `dml.runtime.read_execution_record(...)`.
- Accept either a `Ref` or a `str` input and normalize it to an execution id before reading remote execution state.
- Return the raw execution record typed-dict payload without reshaping or enriching it.
- Preserve existing missing-record and remote-root error behavior from the underlying execution-state reader.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `unified-dml-surface`: extend the shared `Dml.runtime` namespace with a read-only execution-record inspection method.
- `runtime-execution-records`: define the caller-facing read workflow that returns one stored execution record by execution id.

## Impact

- Affected code: `src/daggerml/_core/dml.py`, `src/daggerml/_core/exec_state.py`, and runtime-facing tests.
- Affected API: shared Python runtime namespace on `Dml.runtime`.
- Dependencies/systems: remote execution state under the configured S3-backed `remote.root`.
