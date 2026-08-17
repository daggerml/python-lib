## MODIFIED Requirements

### Requirement: Manual invalidation SHALL target execution identity
Manual invalidation SHALL resolve execution IDs from current `cache/<cache_key>` pointers and compute reverse caller closure from separate edge objects. For each selected execution, the runtime SHALL acquire its embedded lock, CAS-delete the cache pointer only if it still contains that execution ID, and then CAS immutable invalidation metadata into the selected execution record.

#### Scenario: Invalidation deletes pointer before marking
- **WHEN** invalidation targets current execution `e1`
- **THEN** it conditionally deletes `cache/ck1` before storing invalidation requester and timestamp in `execution/e1`

#### Scenario: Rebound pointer is preserved
- **WHEN** invalidation planned `e1` but `cache/ck1` now contains `e2`
- **THEN** invalidation SHALL NOT delete the pointer

### Requirement: Invalidation SHALL write execution tombstones and drop affected cache refs
Invalidation state SHALL be stored once in the unified execution record as `{requested_by, requested_at}`. Readers SHALL reject an invalidated execution even when an interrupted workflow has not yet deleted its cache pointer. Separate invalidation tombstone objects SHALL NOT be created.

#### Scenario: Marked execution is immediately unusable
- **WHEN** `execution/e1` contains invalidation metadata while `cache/ck1` still contains `e1`
- **THEN** cache lookup treats `ck1` as invalidated

### Requirement: Manual cancellation SHALL support `full` and `drive` runtime modes
Cancelation planning and driving SHALL acquire each target execution's embedded lock before changing its record. Phase 1 SHALL store cancelation requester and timestamp, transition lifecycle, remove applicable caller edges, and CAS-delete the cache pointer if it still names the execution. Phase 2 SHALL retain the existing leaves-first lifecycle protocol while locking each execution mutation. The persisted field name SHALL be `cancelation`.

#### Scenario: Cancelation removes current cache binding
- **WHEN** Phase 1 marks current execution `e1` cancel-requested
- **THEN** it conditionally deletes `cache/ck1` if that pointer still contains `e1`

#### Scenario: Cancelation uses stored argv
- **WHEN** the runtime invokes cancelation for `e1`
- **THEN** it reads `argv_ref` from `execution/e1`
- **AND** it does not require an active or cancel-target ref

#### Scenario: Drive mutations require ownership
- **WHEN** Phase 2 changes an execution to cancel-ready or canceled
- **THEN** it holds that execution's matching lock owner and uses CAS

## REMOVED Requirements

### Requirement: Cancellation orphaning SHALL remove current-execution ownership under lock
**Reason**: Embedded locks and direct cache-pointer deletion replace cache-key lock files and active-to-cancel-target moves.
**Migration**: None; the v0 layout is intentionally incompatible.
