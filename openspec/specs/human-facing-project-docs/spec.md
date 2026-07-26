### Requirement: `docs/` SHALL be reserved for human-facing project documentation
The repository SHALL treat `docs/` as the human-facing project documentation surface that describes DaggerML as it exists, while agent-facing change-planning artifacts remain in `openspec/` and maintainer workflow rules live outside `docs/`.

#### Scenario: Human reader enters the docs tree
- **WHEN** a reader opens `docs/`
- **THEN** the visible content describes the product, its usage, its concepts, or its architecture for humans rather than agent workflow or change-planning procedure

#### Scenario: Agent-facing planning remains outside project docs
- **WHEN** a reader needs change proposals, implementation tasks, or requirement deltas for a change
- **THEN** those artifacts are found under `openspec/` rather than inside `docs/`

### Requirement: Documentation SHALL explain why to use DaggerML before audience-specific detail
The docs home SHALL link to a root-level Why DaggerML page that explains the research problems DaggerML addresses, its durable-DAG approach, the outcomes it enables, suitable use cases, and unsuitable use cases.

#### Scenario: Reader evaluates DaggerML
- **WHEN** a prospective reader opens the docs home
- **THEN** they can reach a concise explanation of why DaggerML exists before choosing a documentation path

### Requirement: Project docs SHALL be organized by reader intent
The `docs/` tree SHALL organize its primary navigation by reader relationship to DaggerML: Use DaggerML for researchers, Extend DaggerML for integration engineers, and Develop DaggerML for core contributors. Each path MAY organize detailed material into concepts, guides, and reference pages when that structure serves its readers.

#### Scenario: Reader looks for onboarding
- **WHEN** a new researcher wants the fastest path to first success
- **THEN** the docs home directs them to the top-level getting-started page and the Use DaggerML path

#### Scenario: Reader looks for the right kind of information
- **WHEN** a reader needs to use DaggerML, implement an integration, or develop DaggerML itself
- **THEN** the docs navigation distinguishes those needs through Use, Extend, and Develop paths rather than requiring the reader to start from generic document types or a package subtree

### Requirement: `getting-started` SHALL be one concise page
The project docs SHALL provide one concise researcher getting-started page at `docs/getting-started.md` that covers installation, first repository setup through the CLI, first DAG creation in Python, basic inspection, and next-step links without splitting those basics across multiple introductory files.

#### Scenario: Reader starts from zero
- **WHEN** a reader follows `docs/getting-started.md`
- **THEN** the page includes enough information to install DaggerML, initialize a project with `dml init`, create a first DAG, and inspect it with at least one simple command or API example

### Requirement: Human-facing docs SHALL avoid normative spec voice
Docs under `docs/` SHALL describe the system in reader-facing language and SHALL avoid structuring pages around authority ownership, compatibility classifications, or normative maintenance phrases such as document-level handoff rules.

#### Scenario: Reader opens a topic doc
- **WHEN** a reader opens a concept, guide, reference, or architecture page under `docs/`
- **THEN** the document leads with explanation of the subject matter instead of an authority or governance preamble

### Requirement: Existing technical content SHALL be preserved through translation, not path churn
When current docs are reorganized, the implementation SHALL preserve useful technical knowledge by rewriting and reclassifying existing material into concept, guide, reference, or architecture pages rather than merely renaming files or deleting depth.

#### Scenario: Existing detailed doc is migrated
- **WHEN** a current technical document contains valuable behavioral or architectural explanation
- **THEN** the new docs structure preserves that information in an appropriate human-facing page even if the original path or tone changes

### Requirement: Maintainer workflow docs SHALL leave `docs/`
Repository-maintenance documents such as edit pre-read maps, agent instructions, spec-governance indexes, and contributor test-taxonomy policy SHALL not remain in the human-facing `docs/` tree. Stable contributor setup and architecture material MAY live under the Develop DaggerML path, but automated workflow policy SHALL remain in maintainer-facing locations outside `docs/`.

#### Scenario: Reader encounters maintainer guidance
- **WHEN** a contributor needs edit workflow, agent, or spec-governance guidance
- **THEN** that guidance is located in a maintainer-facing location outside `docs/`

#### Scenario: Contributor needs codebase orientation
- **WHEN** a contributor needs stable architecture or development setup information
- **THEN** they can find it in the Develop DaggerML path without encountering automated maintenance policy there

### Requirement: Docs rewrite tasks SHALL be independently assignable to repo-aware subagents
The reorganization plan SHALL divide implementation work into independent documentation tasks whose owners first inspect the existing repo, current docs, and relevant code for the area they are rewriting.

#### Scenario: Subagent rewrites a docs area
- **WHEN** a subagent is assigned a docs subtree or topic lane
- **THEN** the task instructions require that subagent to read the current docs for that area and inspect the corresponding source modules before producing rewritten docs

#### Scenario: Parallel docs work proceeds safely
- **WHEN** multiple subagents work on different doc lanes such as concepts, reference, architecture, or contrib
- **THEN** the task boundaries are specific enough that each subagent can make progress independently without redefining the whole docs architecture
