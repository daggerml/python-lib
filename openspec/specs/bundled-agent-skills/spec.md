## Purpose

Provide concise, task-focused portable guidance that lets coding agents use DaggerML correctly without receiving an unrelated general-purpose skill.

## Requirements

### Requirement: DaggerML SHALL bundle four focused agent skills
DaggerML SHALL bundle portable Markdown skills named `querying`, `authoring`, `repository`, and `extensions`. Each document SHALL begin with YAML frontmatter naming the skill and describing its purpose, contain no more than 1000 words, and be independently useful when exported without the other skill documents or repository-local documentation and examples. A skill MAY direct readers to installed source modules for deeper investigation.

#### Scenario: A user exports one focused skill
- **WHEN** a user exports one named bundled skill
- **THEN** the output is one complete Markdown document with YAML frontmatter
- **AND** the output does not include the other three skill documents or require repository-local documentation

### Requirement: Authoring skill SHALL guide reproducible DAG construction
The `authoring` skill SHALL concisely cover DAG construction and explicit commit lifecycle; named data staging and retrieval; collection access; direct and staged function calls; importing committed results and named nodes; dagclass composition; script-worker source boundaries; helper injection; provenance-preserving node reuse; cache identity; and complex-data normalization through installed codecs or artifact storage. It SHALL direct agents to pass nodes, projections, required results, and function-call results directly into funks and graph structures instead of materializing them prematurely. It SHALL explain that `.value()` is for inspection or concrete Python computation and include an example that passes a node directly at the authoring boundary before materializing it inside a funk. It SHALL include at most two examples and all operational guidance needed for these workflows without links to repository documentation or examples.

#### Scenario: An agent retrieves authoring guidance
- **WHEN** an agent uses the `authoring` skill to write a script-backed DAG
- **THEN** it is directed to preserve graph identity by passing graph objects directly between funks
- **AND** it sees `.value()` used where worker-side Python computation requires concrete data
- **AND** it is directed to make imports or helper source available to the worker
- **AND** it is warned that cache reuse is based on staged runnable and normalized DaggerML input identity

### Requirement: Repository skill SHALL guide project and shared-state management
The `repository` skill SHALL concisely cover project initialization and configuration, status and history inspection, branches and tags, revision-changing operations, remote synchronization, import-only dependencies, shallow-history boundaries, safe garbage collection, and cache inspection and control. It SHALL direct agents to validate cache identity and retain the exact execution ref before intentional invalidation. It SHALL state that managed `.dml/` state is not for manual modification, include at most two minimal command sequences, and provide all operational guidance without links to repository documentation or examples.

#### Scenario: An agent retrieves repository guidance
- **WHEN** an agent uses the `repository` skill to set up or manage a research project
- **THEN** it is directed to configure the repository and inspect state before mutation
- **AND** it is warned not to run garbage collection concurrently with synchronization

#### Scenario: An agent validates a cached result
- **WHEN** an agent uses the `repository` skill to investigate cache reuse
- **THEN** it is directed to inspect the cache entry and associated execution state
- **AND** intentional invalidation uses the exact execution ref rather than a cache key

### Requirement: Querying skill SHALL guide data extraction and graph traversal
The `querying` skill SHALL concisely cover locating and loading DAGs, distinguishing terminal results from named nodes, traversing nodes and read-only projections, materializing values, following nearest and rooted provenance, and capturing persisted function errors with their context. It SHALL explain any committed, active, or frozen graph-state distinctions necessary to read available data without turning into a repository-control or cache-management guide. It SHALL include at most two minimal traversal examples and provide all operational guidance without links to repository documentation or examples.

#### Scenario: An agent queries a completed DAG
- **WHEN** an agent needs data from a committed DAG
- **THEN** it is directed to select the terminal result or a named node deliberately
- **AND** it can traverse nested data through projections before materializing the selected value

#### Scenario: An agent encounters persisted failure data
- **WHEN** node lookup or materialization encounters a persisted function error
- **THEN** the skill directs the agent to retain the error origin, type, message, stack, and producing context
- **AND** the failure is not treated as absent data

### Requirement: Extensions skill SHALL guide integration development
The `extensions` skill SHALL distinguish adapters as transport boundaries, executors as backend lifecycle implementations, and codecs as deterministic staging normalization. It SHALL cover adapter operations, executor lowering and lifecycle responsibilities, runtime-owned state, response validation, nested runnable forwarding, plugin registration, script-worker source isolation, and contract-first extension testing. It SHALL include at most two minimal examples and provide all operational guidance without links to repository documentation or examples.

#### Scenario: An agent chooses an extension boundary
- **WHEN** an agent designs a new integration
- **THEN** transport behavior is assigned to an adapter, backend launch and teardown to an executor, and value normalization to a codec

#### Scenario: An agent implements an extension lifecycle
- **WHEN** an agent writes or wraps an executor
- **THEN** it is directed to preserve operation context and implement retry-safe, idempotent lifecycle behavior
- **AND** it is directed to validate contracts and plugin discovery before infrastructure-dependent testing
