## MODIFIED Requirements

### Requirement: start_fn mutex-gated adapter dispatch
`IndexOps.start_fn` SHALL implement the following flow on every adapter-backed call:
1. Check cache and return the DAG if hit.
2. Attempt `lock()` and return `None` if it fails.
3. Recheck cache and return the DAG if hit.
4. If no active execution exists for the cache key, reserve a fresh `execution_id` by creating its execution record with `lifecycle = "pending"`.
5. Publish or reuse `active/<cache_key>` only for an execution id that already has an execution record.
6. Record caller/callee dependency state and call the adapter with the active execution id.
7. On terminal success or failure, publish terminal DAG state, clean up the active pointer, and release the lock.
8. On `running`, persist or update launch state, release the lock, and return `None`.

#### Scenario: Fresh launch reserves pending execution before active publication
- **WHEN** `start_fn` observes a cache miss and no active execution for cache key `ck1`
- **THEN** it SHALL create `exec/state/<execution_id>.json` with `lifecycle = "pending"` before publishing `active/ck1`

#### Scenario: Active execution always has a backing execution record
- **WHEN** `start_fn` publishes or reuses `active/ck1`
- **THEN** the referenced `execution_id` SHALL already have `exec/state/<execution_id>.json`

#### Scenario: Missing execution record behind active pointer is stale
- **WHEN** `active/ck1` points at execution `e1`
- **AND** `exec/state/e1.json` does not exist
- **THEN** the runtime SHALL treat `active/ck1` as stale coordination state
- **AND** it SHALL delete or replace that active pointer before continuing normal launch or resume behavior
