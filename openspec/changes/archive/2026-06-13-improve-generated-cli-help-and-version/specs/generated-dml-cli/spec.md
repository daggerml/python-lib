## ADDED Requirements

### Requirement: Generated CLI exposes a root version flag
The generated `dml` CLI SHALL expose a root `--version` flag sourced from the package version metadata.

#### Scenario: Root version flag prints a conventional version string
- **WHEN** a user runs `dml --version`
- **THEN** the CLI prints `dml, version <version>` to `stdout`
- **AND** it exits successfully without requiring a command name

### Requirement: Generated help separates commands from namespaces
Any generated parser that exposes both leaf commands and namespace groups SHALL render them in separate help sections.

#### Scenario: Root help lists commands before namespaces
- **WHEN** a user runs `dml --help`
- **THEN** the help output shows a `commands` section for leaf commands
- **AND** the help output shows a distinct `namespaces` section for namespace groups
- **AND** the `commands` section appears before the `namespaces` section

#### Scenario: Nested namespace help also separates commands from namespaces
- **WHEN** a generated namespace parser exposes both leaf commands and nested namespace groups
- **THEN** its help output shows leaf commands in `commands`
- **AND** it shows nested namespace groups in `namespaces`

#### Scenario: Namespace help text remains visible in the namespace list
- **WHEN** a namespace group appears in generated help
- **THEN** the help output includes that namespace name and its generated help text in the `namespaces` section

#### Scenario: Help rendering preserves parser-managed output behavior
- **WHEN** generated help or version output is shown
- **THEN** the CLI uses the same parser-managed output path as other generated parser help
- **AND** it does not replace that path with manual plain-text printing that would bypass existing formatter behavior
