## ADDED Requirements

### Requirement: Documentation SHALL explain why to use DaggerML before audience-specific detail
The docs home SHALL link to a root-level Why DaggerML page that explains the research problems DaggerML addresses, its durable-DAG approach, the outcomes it enables, suitable use cases, and unsuitable use cases.

#### Scenario: Reader evaluates DaggerML
- **WHEN** a prospective reader opens the docs home
- **THEN** they can reach a concise explanation of why DaggerML exists before choosing a documentation path

## MODIFIED Requirements

### Requirement: Project docs SHALL be organized by reader intent
The `docs/` tree SHALL organize its primary navigation by reader relationship to DaggerML: Use DaggerML for researchers, Extend DaggerML for integration engineers, and Develop DaggerML for core contributors. Each path MAY organize detailed material into concepts, guides, and reference pages when that structure serves its readers.

#### Scenario: Reader looks for onboarding
- **WHEN** a new researcher wants the fastest path to first success
- **THEN** the docs home directs them to the Use DaggerML path and its single getting-started page

#### Scenario: Reader looks for the right kind of information
- **WHEN** a reader needs to use DaggerML, implement an integration, or develop DaggerML itself
- **THEN** the docs navigation distinguishes those needs through Use, Extend, and Develop paths rather than requiring the reader to start from generic document types or a package subtree

### Requirement: `getting-started` SHALL be one concise page
The project docs SHALL provide one concise researcher getting-started page at `docs/use/getting-started.md` that covers installation, first repository setup through the CLI, first DAG creation in Python, basic inspection, and next-step links without splitting those basics across multiple introductory files.

#### Scenario: Reader starts from zero
- **WHEN** a reader follows `docs/use/getting-started.md`
- **THEN** the page includes enough information to install DaggerML, initialize a project with `dml init`, create a first DAG, and inspect it with at least one simple command or API example

### Requirement: Maintainer workflow docs SHALL leave `docs/`
Repository-maintenance documents such as edit pre-read maps, agent instructions, spec-governance indexes, and contributor test-taxonomy policy SHALL not remain in the human-facing `docs/` tree. Stable contributor setup and architecture material MAY live under the Develop DaggerML path, but automated workflow policy SHALL remain in maintainer-facing locations outside `docs/`.

#### Scenario: Reader encounters maintainer guidance
- **WHEN** a contributor needs edit workflow, agent, or spec-governance guidance
- **THEN** that guidance is located in a maintainer-facing location outside `docs/`

#### Scenario: Contributor needs codebase orientation
- **WHEN** a contributor needs stable architecture or development setup information
- **THEN** they can find it in the Develop DaggerML path without encountering automated maintenance policy there
