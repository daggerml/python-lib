## MODIFIED Requirements

### Requirement: Stale lock recovery SHALL preserve active execution ownership
If the current execution's `driver.json.lock` is expired, a caller SHALL attempt to steal that lock by CAS against `driver.json` and resume the same execution ID. It SHALL NOT mutate immutable `metadata.json`, conflate the lock with semantic `state.json`, or create a replacement attempt while the cache pointer still names the existing reusable or resumable execution.

#### Scenario: Expired current execution resumes
- **WHEN** `exec/cache/ck1` contains `e1` and `exec/execution/e1/driver.json.lock` is expired
- **THEN** a caller MAY CAS a new owner into `driver.json.lock`
- **AND** it resumes `e1` without replacing `metadata.json` or `state.json`
