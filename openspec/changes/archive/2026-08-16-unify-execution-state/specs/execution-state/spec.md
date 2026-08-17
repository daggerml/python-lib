## ADDED Requirements

### Requirement: Execution mutations SHALL be serialized by embedded owner locks
Each `execution/<execution_id>` record SHALL contain `lock = null` or `lock = {owner: str, ttl: float}`. Lock acquisition SHALL use compare-and-swap to replace a null or expired lock with a fresh UUID4 owner. Every execution-record mutation other than lock acquisition SHALL require the current lock owner and SHALL use compare-and-swap against the latest ETag. Unlock SHALL compare-and-swap the lock to null only when the stored owner matches the unlocking owner.

#### Scenario: One caller acquires an unlocked execution
- **WHEN** two callers concurrently attempt to replace a null execution lock
- **THEN** exactly one conditional update succeeds
- **AND** the successful record stores that caller's owner UUID

#### Scenario: Stale owner cannot mutate after a steal
- **WHEN** an expired lock owned by `o1` is conditionally replaced by owner `o2`
- **THEN** a mutation from `o1` fails its stale compare-and-swap
- **AND** `o1` stops after rereading owner `o2`

#### Scenario: Stale unlock preserves replacement owner
- **WHEN** owner `o1` attempts to unlock after owner `o2` has stolen the lock
- **THEN** the runtime SHALL NOT clear `o2`'s lock

### Requirement: Lock expiry SHALL use S3 response time
The runtime SHALL determine lock expiry using `LastModified + lock.ttl <= Date`, where `LastModified` and HTTP `Date` come from the same S3 execution-record response. It SHALL NOT use caller wall-clock time for that decision. Expiry SHALL permit lock stealing but SHALL NOT revoke an unchanged owner by itself.

#### Scenario: S3 timestamps report an expired lock
- **WHEN** an execution response has `LastModified + lock.ttl <= Date`
- **THEN** another caller MAY attempt to replace the lock owner by compare-and-swap

#### Scenario: Expired owner remains authoritative until stolen
- **WHEN** an adapter response arrives after the lock TTL
- **AND** the execution record still contains the caller's owner UUID
- **THEN** that caller MAY persist the response by compare-and-swap

#### Scenario: Owner mutation refreshes lease basis
- **WHEN** the lock owner successfully mutates the execution record
- **THEN** S3 updates `LastModified`
- **AND** subsequent expiry checks SHALL use that updated timestamp

### Requirement: Cache resolution SHALL coordinate one current execution
On a cache miss, the runtime SHALL create a fresh UUID7 execution record with a fresh owner lock before conditionally creating `cache/<cache_key>` containing only that execution ID. If cache-pointer creation conflicts, the runtime SHALL conditionally delete only its unchanged new execution record and SHALL reread the winning cache pointer. UUID ordering SHALL NOT select the winner.

#### Scenario: Concurrent cache miss has one winner
- **WHEN** multiple callers create different execution records for one absent cache key
- **THEN** S3 conditional cache-pointer creation selects exactly one current execution
- **AND** losing callers reread that winner

#### Scenario: Execution exists before pointer publication
- **WHEN** a caller successfully creates `cache/ck1` containing `e1`
- **THEN** `execution/e1` already exists

#### Scenario: Lost claim cleans only the losing record
- **WHEN** execution `e2` loses cache-pointer creation to execution `e1`
- **THEN** the `e2` caller conditionally deletes its unchanged execution record
- **AND** it does not modify `e1`

## REMOVED Requirements

### Requirement: S3-backed mutex lock file
**Reason**: The lock is embedded in the unified execution record.
**Migration**: None; the v0 layout is intentionally incompatible.

### Requirement: Lock acquired via create-if-absent
**Reason**: Lock acquisition is now a CAS update of the execution record.
**Migration**: None; the v0 layout is intentionally incompatible.

### Requirement: Lock released via DELETE
**Reason**: Unlock now CAS-updates the execution record's lock to null.
**Migration**: None; the v0 layout is intentionally incompatible.

### Requirement: Caller-owned launch state SHALL be serialized by cache-key lock
**Reason**: Adapter state and the owner lock now reside in one execution record.
**Migration**: None; the v0 layout is intentionally incompatible.
