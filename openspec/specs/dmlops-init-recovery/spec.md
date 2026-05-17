## Purpose
Define the config-first repository bootstrap recovery behavior that the shared internal `Dml` entrypoint must preserve when project config exists but local DB state is missing.

## Requirements

### Requirement: Init recovers missing DB when project config already exists
The system SHALL treat `.dml/config.toml` + missing `.dml/db/` as a recoverable initialization state through the shared internal `Dml` bootstrap workflow.

#### Scenario: Existing config with missing DB is recovered
- **WHEN** the `Dml` init/bootstrap workflow runs in a project where `.dml/config.toml` exists and `.dml/db/` does not
- **THEN** initialization uses `dml_context` to resolve bootstrap context, creates `.dml/db/`, and completes without requiring manual repository repair

### Requirement: Recovery mode pulls when a project URI is configured
The system SHALL perform project bootstrap pull during recovery when resolved configuration includes `remote.project`.

#### Scenario: Recovery triggers pull when project URI is present
- **WHEN** the `Dml` init/bootstrap workflow recovers a missing DB and resolved config includes `remote.project`
- **THEN** it uses `dml_context` to obtain the resolved project and remote configuration and runs pull through the relevant ops-backed workflow to populate local repository state

#### Scenario: Recovery skips pull when project URI is absent
- **WHEN** the `Dml` init/bootstrap workflow recovers a missing DB and resolved config has no `remote.project`
- **THEN** it creates local DB state without invoking pull
