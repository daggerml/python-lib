## ADDED Requirements

### Requirement: Runtime SHALL return raw execution records for direct record reads
The runtime SHALL support a direct single-record read workflow for execution records addressed by execution id. When a caller-facing runtime inspection method requests one execution record, the execution-state layer SHALL read only `exec/state/<execution_id>.json` for that id and SHALL return the stored `execution_record` typed dict unchanged.

#### Scenario: Direct record read returns the stored execution record unchanged
- **WHEN** `exec/state/e1.json` exists for execution `e1`
- **THEN** the runtime SHALL return the stored execution record for `e1`
- **AND** the returned payload SHALL preserve `execution_id`, `cache_key`, `lifecycle`, `updated_at`, `created_at`, `spawned_execution_ids`, `child_execution_ids`, and `cancellation_requested_by` exactly as stored

#### Scenario: Direct record read does not reshape into a graph or summary payload
- **WHEN** a caller reads execution record `e1`
- **THEN** the runtime SHALL NOT synthesize `children`, `spawned`, or any other derived inspection fields outside the stored execution-record schema

#### Scenario: Direct record read surfaces missing-record failure
- **WHEN** a caller reads execution record `missing`
- **AND** `exec/state/missing.json` does not exist
- **THEN** the runtime SHALL raise `DmlRepoError`
