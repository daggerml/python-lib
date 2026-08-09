## Purpose
Define the config-first repository bootstrap recovery behavior that the shared internal `Dml` entrypoint must preserve when project config exists but local DB state is missing.

## Requirements

### Requirement: Init recovers missing DB when project config already exists
The system SHALL treat `.dml/config.json` plus missing `.dml/db/` as recoverable initialization state through the shared `Dml` bootstrap workflow.

#### Scenario: Existing JSON config with missing DB is recovered
- **WHEN** bootstrap finds `.dml/config.json` and no `.dml/db/`
- **THEN** it resolves config, creates DB state, and completes without manual repair

### Requirement: Recovery fetches bootstrap state when remote root is configured
The system SHALL fetch and check out bootstrap state during missing-DB recovery only when resolved configuration includes `remote.root`.

#### Scenario: Recovery fetches state when remote root is present
- **WHEN** recovery creates a missing DB and resolved config includes `remote.root`
- **THEN** it fetches branch `default.branch_name` from that root and checks it out locally

#### Scenario: Recovery remains local without remote root
- **WHEN** recovery creates a missing DB and resolved config has no `remote.root`
- **THEN** it creates local DB state without fetch, pull, or checkout
