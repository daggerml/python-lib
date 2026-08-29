## ADDED Requirements

### Requirement: CLI explicit override names mirror canonical config parameters
The CLI SHALL name explicit configuration override flags after the canonical parameters they populate in the shared internal resolver whenever those parameters are exposed directly to users.

#### Scenario: Project-home flag maps to canonical parameter
- **WHEN** the CLI resolves an explicit local project path override
- **THEN** it reads that value from a flag named after `project.home`
- **AND** it forwards the value into shared resolution as `project.home`

#### Scenario: Remote-uri flag maps to canonical parameter
- **WHEN** the CLI resolves an explicit remote project override
- **THEN** it reads that value from a flag named after `remote.uri`
- **AND** it forwards the value into shared resolution as `remote.uri`

#### Scenario: Existing canonical names remain unchanged
- **WHEN** the CLI exposes other explicit config-shaped overrides such as `--remote-project` or `--config-home`
- **THEN** those flags continue using the established canonical names rather than introducing alternate aliases
