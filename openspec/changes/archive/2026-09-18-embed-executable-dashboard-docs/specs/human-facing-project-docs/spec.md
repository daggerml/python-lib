## MODIFIED Requirements

### Requirement: `docs/` SHALL be reserved for human-facing project documentation
The repository SHALL treat `docs/` as the canonical human-facing Quarto QMD documentation source describing DaggerML as it exists, published inside the dashboard. Agent-facing change-planning artifacts SHALL remain in `openspec/` and maintainer workflow rules SHALL live outside `docs/`.

#### Scenario: Human reader enters the docs tree
- **WHEN** a reader opens `docs/` or dashboard Docs
- **THEN** the visible content describes the product, its usage, its concepts, or its architecture for humans rather than agent workflow or change-planning procedure

#### Scenario: Agent-facing planning remains outside project docs
- **WHEN** a reader needs change proposals, implementation tasks, or requirement deltas for a change
- **THEN** those artifacts are found under `openspec/` rather than inside `docs/`

### Requirement: Project docs SHALL be organized by reader intent
The `docs/` tree SHALL organize its primary navigation by reader relationship to DaggerML: Use DaggerML for researchers, Extend DaggerML for integration engineers, and Develop DaggerML for core contributors. Each path MAY organize detailed material into concepts, guides, and reference pages when that structure serves its readers. An Examples subtree SHALL embed curated Python examples with explanations and download links; shell-oriented push/pull workflows SHALL remain outside this embedded collection.

#### Scenario: Reader looks for onboarding
- **WHEN** a new researcher wants the fastest path to first success
- **THEN** the docs home directs them to the top-level getting-started page and the Use DaggerML path

#### Scenario: Reader looks for the right kind of information
- **WHEN** a reader needs to use DaggerML, implement an integration, or develop DaggerML itself
- **THEN** the docs navigation distinguishes those needs through Use, Extend, and Develop paths rather than requiring the reader to start from generic document types or a package subtree

#### Scenario: Reader wants a worked example
- **WHEN** a reader opens the Examples subtree
- **THEN** they can read focused lessons with executed Python source and download the corresponding scripts

### Requirement: `getting-started` SHALL be one concise page
The project docs SHALL provide one concise researcher getting-started page at `docs/getting-started.qmd`, published in dashboard Docs, covering installation, first repository setup through the CLI, first DAG creation in Python, basic inspection, and next-step links without splitting those basics across multiple introductory files. Unlike ordinary examples, this page SHALL visibly teach required initialization rather than hide it as fixture plumbing.

#### Scenario: Reader starts from zero
- **WHEN** a reader follows the getting-started page
- **THEN** the page includes enough information to install DaggerML, initialize a project with `dml init`, create a first DAG, and inspect it with at least one simple command or API example
