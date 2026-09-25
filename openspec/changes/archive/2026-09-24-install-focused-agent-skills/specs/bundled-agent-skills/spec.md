## MODIFIED Requirements

### Requirement: DaggerML SHALL bundle four focused agent skills
DaggerML SHALL bundle portable skill directories named `querying`, `authoring`, `repository`, and `extensions`, each containing a `SKILL.md` document and any supporting examples. Each document SHALL begin with YAML frontmatter naming the skill and describing its purpose, be independently useful when installed without the other skill documents or repository-local documentation, and support substituting a user-chosen installation name in its frontmatter. A skill MAY direct readers to installed source modules for deeper investigation.

#### Scenario: A user installs one focused skill
- **WHEN** a user installs one named bundled skill
- **THEN** the installed document contains YAML frontmatter with its installed name
- **AND** it does not include the other three skill documents or require repository-local documentation

### Requirement: Authoring skill SHALL guide reproducible DAG construction
The `authoring` skill SHALL cover DAG construction and explicit commit lifecycle; named data staging and retrieval; collection access; direct and staged function calls; importing committed results and named nodes; dagclass composition; script-worker source boundaries; helper injection; provenance-preserving node reuse; cache identity; and complex-data normalization through installed codecs or artifact storage. It SHALL provide practical, self-contained workflows for authoring, composing, and diagnosing script-backed DAGs. It SHALL direct agents to pass nodes, projections, required results, and function-call results directly into funks and graph structures instead of materializing them prematurely. It SHALL explain that `.value()` is for inspection or concrete Python computation and include an example that passes a node directly at the authoring boundary before materializing it inside a funk. It SHALL provide operational guidance without requiring repository-local documentation or examples.

#### Scenario: An agent retrieves authoring guidance
- **WHEN** an agent uses the `authoring` skill to write a script-backed DAG
- **THEN** it is directed to preserve graph identity by passing graph objects directly between funks
- **AND** it sees `.value()` used where worker-side Python computation requires concrete data
- **AND** it is directed to make imports or helper source available to the worker
- **AND** it is warned that cache reuse is based on staged runnable and normalized DaggerML input identity

#### Scenario: An agent authors a dagclass
- **WHEN** an agent uses the `authoring` skill to define a dagclass
- **THEN** it finds an installed Python example that stages airline delays, returns Polars train/test DataFrames for codec normalization, trains and persists tree models, predicts and scores both cuts, and searches over parameter sets by out-of-sample R²
- **AND** it can see how the dagclass retains parameter/objective pairs as nodes, prints the best parameters and score, and uses `.value()` only where Python computation needs concrete data

### Requirement: Querying skill SHALL guide data extraction and graph traversal
The `querying` skill SHALL cover locating and loading DAGs, distinguishing terminal results from named nodes, traversing nodes and read-only projections, materializing values, following nearest and rooted provenance, and capturing persisted function errors with their context. It SHALL provide actionable workflows for selecting data, interpreting graph state, and investigating failed results using Python or available CLI inspection commands. It SHALL explain committed, active, and frozen graph-state distinctions necessary to read available data without turning into a repository-control or cache-management guide. It SHALL provide operational guidance without requiring repository-local documentation or examples.

#### Scenario: An agent queries a completed DAG
- **WHEN** an agent needs data from a committed DAG
- **THEN** it is directed to select the terminal result or a named node deliberately
- **AND** it can traverse nested data through projections before materializing the selected value

#### Scenario: An agent encounters persisted failure data
- **WHEN** node lookup or materialization encounters a persisted function error
- **THEN** the skill directs the agent to retain the error origin, type, message, stack, and producing context
- **AND** the failure is not treated as absent data
