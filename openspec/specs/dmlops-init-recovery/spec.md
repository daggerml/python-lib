### Requirement: Init recovers missing DB when project config already exists
The system SHALL treat `.dml/config.toml` + missing `.dml/db/` as a recoverable initialization state.

#### Scenario: Existing config with missing DB is recovered
- **WHEN** `DmlOps.init` runs in a project where `.dml/config.toml` exists and `.dml/db/` does not
- **THEN** initialization creates `.dml/db/` and completes without requiring manual repository repair

### Requirement: Recovery mode pulls when a project URI is configured
The system SHALL perform project bootstrap pull during recovery when resolved configuration includes `project.uri`.

#### Scenario: Recovery triggers pull when project URI is present
- **WHEN** `DmlOps.init` recovers a missing DB and resolved config includes `project.uri`
- **THEN** it runs pull using the resolved project and remote configuration to populate local repository state

#### Scenario: Recovery skips pull when project URI is absent
- **WHEN** `DmlOps.init` recovers a missing DB and resolved config has no `project.uri`
- **THEN** it creates local DB state without invoking pull
