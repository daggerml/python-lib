## ADDED Requirements

### Requirement: CLI project and remote override flags use canonical config-shaped names
The CLI SHALL expose explicit project and remote override flags using the canonical configuration naming represented by the shared resolver, rather than frontend-specific aliases.

#### Scenario: Top-level project override uses canonical name
- **WHEN** a user passes an explicit project directory to any command
- **THEN** the CLI accepts `--project-home <path>` as the top-level override flag
- **AND** the CLI does not advertise `--repo` as the supported flag name

#### Scenario: Top-level remote override uses canonical name
- **WHEN** a user passes an explicit remote project URI to any command
- **THEN** the CLI accepts `--remote-uri <uri>` as the top-level override flag
- **AND** the CLI does not advertise `--remote-root` as the supported flag name

### Requirement: CLI guidance uses canonical flag names consistently
CLI help text, examples, and normalized user-facing recovery hints SHALL use the same canonical flag names as the parser surface.

#### Scenario: Help examples show canonical overrides
- **WHEN** a user opens top-level or subcommand help for commands that mention explicit config overrides
- **THEN** the examples and help text refer to `--project-home` and `--remote-uri` instead of legacy aliases

#### Scenario: Missing project-home hint uses canonical flag name
- **WHEN** command execution fails because no local project path can be resolved
- **THEN** the structured error hint instructs the user to pass `--project-home PATH` or set `DML_PROJECT_HOME`

### Requirement: Shared public flag names do not create ambiguous CLI behavior
When the CLI uses the same canonical public flag spelling in different parser scopes, command dispatch SHALL preserve the intended meaning for each command path.

#### Scenario: Init keeps its own remote-uri input without shadowing the top-level override
- **WHEN** the CLI exposes both a top-level `--remote-uri` option and `init --remote-uri`
- **THEN** parsing and command execution keep those inputs distinguishable
- **AND** `init` continues to forward its own `--remote-uri` value to bootstrap project remote configuration
