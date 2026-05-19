## ADDED Requirements

### Requirement: `docs/` SHALL be reserved for human-facing project documentation
The repository SHALL treat `docs/` as the human-facing project documentation surface that describes DaggerML as it exists, while agent-facing change-planning artifacts remain in `openspec/` and maintainer workflow rules live outside `docs/`.

#### Scenario: Human reader enters the docs tree
- **WHEN** a reader opens `docs/`
- **THEN** the visible content describes the product, its usage, its concepts, or its architecture for humans rather than agent workflow or change-planning procedure

#### Scenario: Agent-facing planning remains outside project docs
- **WHEN** a reader needs change proposals, implementation tasks, or requirement deltas for a change
- **THEN** those artifacts are found under `openspec/` rather than inside `docs/`

### Requirement: Project docs SHALL be organized by reader intent
The `docs/` tree SHALL organize its primary navigation around reader intent with a docs home, one getting-started page, concept docs, guides, reference docs, architecture docs, and a contrib subtree using the same broad model.

#### Scenario: Reader looks for onboarding
- **WHEN** a new reader wants the fastest path to first success
- **THEN** `docs/README.md` points to a single `docs/getting-started.md` page rather than a fragmented getting-started subtree

#### Scenario: Reader looks for the right kind of information
- **WHEN** a reader wants a mental model, a workflow, an exact command surface, or an internal system explanation
- **THEN** the docs navigation distinguishes those needs through concepts, guides, reference, and architecture sections

### Requirement: `getting-started` SHALL be one concise page
The project docs SHALL provide a single getting-started page that covers installation, first repository setup, first DAG creation, basic inspection, and next-step links without splitting those basics across multiple introductory files.

#### Scenario: Reader starts from zero
- **WHEN** a reader follows `docs/getting-started.md`
- **THEN** the page includes enough information to install DaggerML, create or select a repo, create a first DAG, and inspect it with at least one simple command or API example

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
Repository-maintenance documents such as edit pre-read maps, spec-governance indexes, and contributor test-taxonomy policy SHALL not remain in the human-facing `docs/` tree after the reorganization.

#### Scenario: Reader encounters maintainer guidance
- **WHEN** a maintainer needs edit workflow or contributor-policy guidance
- **THEN** that guidance is located in a maintainer-facing location outside `docs/`

### Requirement: Docs rewrite tasks SHALL be independently assignable to repo-aware subagents
The reorganization plan SHALL divide implementation work into independent documentation tasks whose owners first inspect the existing repo, current docs, and relevant code for the area they are rewriting.

#### Scenario: Subagent rewrites a docs area
- **WHEN** a subagent is assigned a docs subtree or topic lane
- **THEN** the task instructions require that subagent to read the current docs for that area and inspect the corresponding source modules before producing rewritten docs

#### Scenario: Parallel docs work proceeds safely
- **WHEN** multiple subagents work on different doc lanes such as concepts, reference, architecture, or contrib
- **THEN** the task boundaries are specific enough that each subagent can make progress independently without redefining the whole docs architecture
