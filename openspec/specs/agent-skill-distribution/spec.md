## Purpose

Provide installed DaggerML users with concise, portable coding-agent guidance that stays matched to their installed version.

## Requirements

### Requirement: The package distributes a portable agent skill
The DaggerML distribution SHALL include a Markdown agent skill document retrievable by supported DaggerML tooling. The document SHALL begin with YAML frontmatter containing `name` and `description`, and its body SHALL be portable across coding-agent tools without requiring tool-specific configuration syntax.

#### Scenario: Retrieved skill has standard metadata
- **WHEN** a user retrieves the bundled agent skill
- **THEN** the output begins with YAML frontmatter containing non-empty `name` and `description` fields
- **AND** the remainder is a Markdown skill document

### Requirement: The agent skill concisely orients DaggerML authoring
The bundled agent skill SHALL concisely explain the DaggerML environment and CLI workflow, the mutable-DAG to committed-result model, the distinction between Python authoring and CLI administration, and the `dag.put`, `dag.call`, `dag.commit`, and `dml.load` authoring operations. It SHALL include small Python and shell examples.

#### Scenario: Agent reads the skill before working in a DaggerML project
- **WHEN** a coding agent reads the retrieved skill
- **THEN** it can determine that it must use the environment containing the `dml` executable
- **AND** it can initialize and inspect a project with the CLI
- **AND** it can identify the basic Python operations for authoring and inspecting a DAG

### Requirement: The agent skill documents funk execution boundaries
The bundled agent skill SHALL state that `@api.funkify` functions execute as delayed work, receive node-like inputs in their worker context, and must materialize those inputs with `.value()` when required. It SHALL state that script workers receive function source rather than module globals and SHALL direct authors to import dependencies inside the function or explicitly provide them through supported injection options. It SHALL state that remote-backed execution and cache coordination require `remote.root`.

#### Scenario: Agent authors a script funk from the skill
- **WHEN** a coding agent uses the skill to author a script-backed funk
- **THEN** the funk materializes node-like inputs before using their values
- **AND** it does not rely on module-level imports or globals being implicitly available in the worker

### Requirement: The agent skill states managed-project boundaries
The bundled agent skill SHALL direct agents to use DaggerML tooling for project, history, remote, runtime, and administration operations and SHALL prohibit manually modifying DaggerML-managed objects or refs.

#### Scenario: Agent needs repository maintenance guidance
- **WHEN** a coding agent reads the agent skill before changing DaggerML project state
- **THEN** it uses `dml` commands for administrative and inspection operations
- **AND** it does not directly modify managed objects or refs

### Requirement: The agent skill presents actionable sharp bits
The bundled agent skill SHALL concisely warn that `remote.root` is required before mutable DAG operations, that editable imported helper implementations are excluded from script-funk cache identity unless explicitly included, and that administrative commands MUST NOT run concurrently with pull or remote synchronization against the same project. It SHALL include examples showing an explicit helper-source injection and the non-concurrent pull/admin commands.

#### Scenario: Agent consults sharp-bit guidance before authoring or maintenance
- **WHEN** a coding agent reads the agent skill before mutating a project or editing a script funk
- **THEN** it does not attempt mutable DAG operations without `remote.root`
- **AND** it can include an editable helper in the funk's cache identity
- **AND** it does not run administrative work concurrently with pull or synchronization
