## MODIFIED Requirements

### Requirement: Recovery mode pulls when a project URI is configured
The system SHALL fetch and check out project bootstrap state during recovery only when resolved configuration includes `remote.project`.

#### Scenario: Recovery fetches project state when project URI is present
- **WHEN** the `Dml` init/bootstrap workflow recovers a missing DB and resolved config includes `remote.project`
- **THEN** it uses resolved remote and project configuration to fetch project state and check out the fetched revision locally

#### Scenario: Recovery skips fetch and checkout when project URI is absent
- **WHEN** the `Dml` init/bootstrap workflow recovers a missing DB and resolved config has no `remote.project`
- **THEN** it creates local DB state without invoking project fetch, pull, or checkout
