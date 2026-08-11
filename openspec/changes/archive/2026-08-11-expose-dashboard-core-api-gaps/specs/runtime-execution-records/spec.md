## ADDED Requirements

### Requirement: Runtime SHALL expose persisted executor resume state for direct reads
The runtime SHALL support a direct launch-state read addressed by execution id. Persisted executor `resume_state` SHALL be a JSON object, and the read SHALL return that object as a `dict` without returning or combining execution lifecycle data. If no launch-state object exists for that execution id, the read SHALL return `None`. A stored launch-state object whose `resume_state` is not a JSON object, including JSON `null`, SHALL be treated as malformed state and fail rather than being conflated with absence.

#### Scenario: Direct launch-state read returns resume state unchanged
- **WHEN** execution `e1` has caller-owned launch state whose `resume_state` is `{"job_id": "j1"}`
- **THEN** a direct launch-state read for `e1` returns `{"job_id": "j1"}` unchanged

#### Scenario: Direct launch-state read excludes lifecycle data
- **WHEN** a caller reads launch state for execution `e1`
- **THEN** the result does not synthesize lifecycle, lineage, graph, or execution-record fields

#### Scenario: Missing launch state returns none
- **WHEN** no caller-owned launch-state object exists for execution `missing`
- **THEN** a direct launch-state read for `missing` returns `None`

#### Scenario: Non-object resume state fails closed
- **WHEN** a stored launch-state object contains a scalar, array, or `null` `resume_state`
- **THEN** a direct launch-state read fails as malformed persisted state
- **AND** it does not return `None`
