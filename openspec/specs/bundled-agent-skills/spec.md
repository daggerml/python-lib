## Purpose

Provide concise, task-focused portable guidance that lets coding agents use DaggerML correctly without receiving an unrelated general-purpose skill.

## Requirements

### Requirement: DaggerML SHALL bundle three focused agent skills
DaggerML SHALL bundle portable Markdown skills named `authoring`, `repository`, and `inspection`. Each document SHALL begin with YAML frontmatter naming the skill and describing its purpose, and SHALL be independently useful when exported without either of the other skills or repository-local documentation and examples. A skill MAY direct readers to installed source modules for deeper investigation.

#### Scenario: A user exports one focused skill
- **WHEN** a user exports one named bundled skill
- **THEN** the output is one complete Markdown document with YAML frontmatter
- **AND** the output does not include the other two skill documents or require repository-local documentation

### Requirement: Authoring skill SHALL guide reproducible DAG construction
The `authoring` skill SHALL concisely cover DAG construction and commit lifecycle, node materialization, function-call naming, script-worker source boundaries, helper injection, remote prerequisites, provenance-preserving reuse, and cache identity. It SHALL include at most two minimal examples and all operational guidance needed for these workflows without links to repository documentation or examples.

#### Scenario: An agent retrieves authoring guidance
- **WHEN** an agent uses the `authoring` skill to write a script-backed DAG
- **THEN** it is directed to materialize node-like inputs in the worker and to make imports or helper source available to that worker
- **AND** it is warned that cache reuse is based on staged runnable and normalized DaggerML input identity

### Requirement: Repository skill SHALL guide Git-like research management
The `repository` skill SHALL concisely cover status and history inspection, branches and tags, revision-changing operations, remote synchronization, import-only dependencies, shallow-history boundaries, and safe garbage collection. It SHALL state that managed `.dml/` state is not for manual modification, include at most one minimal command sequence, and provide all operational guidance without links to repository documentation or examples.

#### Scenario: An agent retrieves repository guidance
- **WHEN** an agent uses the `repository` skill to manage a research project
- **THEN** it is directed to inspect repository state before history changes
- **AND** it is warned not to run garbage collection concurrently with synchronization

### Requirement: Inspection skill SHALL guide durable and in-progress graph analysis
The `inspection` skill SHALL concisely distinguish committed DAGs, frozen runtimes, active runtimes, and remote executions. It SHALL cover loading and traversing a committed graph; node, error, and provenance inspection; runtime and execution-lineage inspection; and intentional cache invalidation by exact execution ref. It SHALL include at most one minimal traversal example and provide all operational guidance without links to repository documentation or examples.

#### Scenario: An agent retrieves inspection guidance
- **WHEN** an agent investigates a completed function-call result
- **THEN** it is directed to traverse from the named node into its producing function or import DAG
- **AND** it is directed to inspect persisted error context rather than treating a failure as absent data
