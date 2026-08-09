## MODIFIED Requirements

### Requirement: Init accepts optional remote capabilities
The init operation MUST accept optional `remote_root`. It MUST allow that value to be omitted for local-only repository bootstrap and MUST NOT accept `remote_project`.

#### Scenario: Init without remote configuration
- **WHEN** init is called without `remote_root`
- **THEN** init succeeds with remote-backed synchronization and execution unavailable

#### Scenario: Init with remote root
- **WHEN** init is called with `remote_root`
- **THEN** init succeeds and configures project synchronization, CAS, cache, and execution capability at that root

## REMOVED Requirements

### Requirement: Init rejects project identity without remote root
**Reason**: Init no longer accepts project identity.
**Migration**: Remove `remote_project`; supply `remote_root` only when remote capability is wanted.
