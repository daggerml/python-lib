## MODIFIED Requirements

### Requirement: Remote-aware components require explicit remote configuration
The system SHALL require explicit normalized endpoint configuration at the constructor or helper boundary for any runtime or ops component that performs remote-backed behavior. Project synchronization, CAS, cache, and execution coordination components MUST receive resolved `remote.root`; dependency fetch components MUST receive the selected dependency config. Components SHALL NOT resolve endpoint files or environment variables directly.

#### Scenario: Project remote-aware ops requires remote root
- **WHEN** an ops type performs project synchronization, CAS, cache, or execution behavior
- **THEN** its constructor requires concrete normalized `remote.root` configuration

#### Scenario: Dependency helper passes selected config
- **WHEN** a helper fetches dependency `models`
- **THEN** it resolves `.dml/refs/dep/models/config.json` and passes the normalized endpoint config to the remote-aware component

#### Scenario: Remote-aware component does not resolve configuration directly
- **WHEN** a remote-aware runtime or ops component is used
- **THEN** it receives already-resolved endpoint configuration instead of reading local config or environment variables

#### Scenario: Init permits absent remote root
- **WHEN** local-only initialization has no valid `remote.root`
- **THEN** initialization succeeds but remote-backed synchronization and execution remain unavailable

## REMOVED Requirements

### Requirement: Project sync operations require project identity in addition to remote root
**Reason**: One `remote.root` identifies the project synchronization and execution endpoint; no separate project identity exists.
**Migration**: Remove `remote.project` and configure the one-project endpoint directly as `remote.root`.
