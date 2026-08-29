## MODIFIED Requirements

### Requirement: ExecutionState constructed from remote_root
The system SHALL accept `remote_root: str` as a required configuration parameter for `ExecutionState`. Call sites that construct `ExecutionState` MUST provide a valid remote root explicitly and MUST NOT rely on optional remote-root values or `None` defaults.

#### Scenario: remote_root parsed to bucket and prefix
- **WHEN** `ExecutionState(cache_key, remote_root="s3://my-bucket/my/prefix")` is constructed
- **THEN** lock operations target `s3://my-bucket/my/prefix/exec/{cache_key}.json`

#### Scenario: call site provides explicit remote_root
- **WHEN** code constructs `ExecutionState` for a remote-backed execution flow
- **THEN** that call site passes a concrete `remote_root: str` value at construction time
