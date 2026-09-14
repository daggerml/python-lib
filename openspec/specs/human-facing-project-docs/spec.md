## Purpose
Define the audience, organization, and boundaries of human-facing project documentation.

## Requirements

### Requirement: `docs/` SHALL be reserved for human-facing project documentation
The repository SHALL treat `docs/` as the human-facing project documentation surface that describes DaggerML as it exists, while agent-facing change-planning artifacts remain in `openspec/` and maintainer workflow rules live outside `docs/`.

#### Scenario: Human reader enters the docs tree
- **WHEN** a reader opens `docs/`
- **THEN** the visible content describes the product, its usage, its concepts, or its architecture for humans rather than agent workflow or change-planning procedure

#### Scenario: Agent-facing planning remains outside project docs
- **WHEN** a reader needs change proposals, implementation tasks, or requirement deltas for a change
- **THEN** those artifacts are found under `openspec/` rather than inside `docs/`

### Requirement: The documentation home SHALL explain why to use DaggerML
The single docs home SHALL concisely explain the research problems DaggerML addresses, its durable-DAG approach, the outcomes it enables, suitable use cases, and unsuitable use cases before routing readers into detail.

#### Scenario: Reader evaluates DaggerML
- **WHEN** a prospective reader opens the docs home
- **THEN** the home itself explains why DaggerML exists before presenting the documentation paths

### Requirement: Project docs SHALL use a minimal primary navigation
Primary navigation SHALL contain Start here, Concepts, Extend, and Develop sections. Glossary and Sharp bits and security SHALL be direct top-level siblings of those sections rather than additional containers.

#### Scenario: Reader looks for onboarding
- **WHEN** a new researcher wants the fastest path to first success
- **THEN** the docs home directs them into the ordered Start here course

#### Scenario: Reader looks for the right kind of information
- **WHEN** a reader needs to use DaggerML, implement an integration, or develop DaggerML itself
- **THEN** the docs navigation distinguishes executable learning, explanatory concepts, extension work, and core development without a separate Examples or Use section

### Requirement: Documentation source paths SHALL mirror published paths
Human-facing QMD source paths SHALL match their published HTML paths as closely as the rendering system permits. Every page presented in the Start here navigation section SHALL live beneath `docs/start-here/` and publish beneath `/docs/start-here/`; supporting downloadable source MAY remain beneath `docs/examples/`.

#### Scenario: Maintainer locates a Start here page
- **WHEN** a maintainer maps a `/docs/start-here/...` route back to its QMD source
- **THEN** the corresponding page is found at the same relative path beneath `docs/start-here/`

#### Scenario: Reader opens the documentation root
- **WHEN** a reader opens `/docs`
- **THEN** the dashboard presents the `docs/start-here/index.qmd` page without requiring a duplicate root-level QMD

### Requirement: Start here SHALL be an executable course
Start here SHALL progress from the docs home through repository setup, DAGs, funks, and dagclasses. Pages SHALL declare their prerequisites with `depends-on`; the documentation build and navigation SHALL use the same validated topological order.

#### Scenario: Reader starts from zero
- **WHEN** a reader follows `docs/start-here/get-started.qmd`
- **THEN** Get started initializes the project and later pages execute the important authoring, access, Docker, function, cache, and dagclass examples against that shared project

#### Scenario: A learning example stops working
- **WHEN** an important executable example in Start here fails
- **THEN** the documentation build and CI fail rather than publishing stale output

### Requirement: Concept documentation SHALL be prose with explicit pseudocode
Concept pages SHALL explain behavior in prose. Code used to illustrate a concept SHALL be visibly non-executable and explicitly marked as pseudocode; runnable teaching belongs in Start here.

#### Scenario: Reader distinguishes explanation from verified instruction
- **WHEN** a reader sees code on a concept page
- **THEN** it is presented as illustrative pseudocode rather than implied to have executed during the documentation build

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

### Requirement: Executable docs SHALL not use ambient user state
The executable course SHALL run in a fresh build workspace with isolated DaggerML and cloud configuration. A page with `dml-project-home` SHALL run from that workspace-relative directory only after a declared dependency has created it.

#### Scenario: A developer builds the documentation
- **WHEN** the complete documentation build succeeds or fails
- **THEN** its repositories, credentials, configuration, services, and temporary state are removed without modifying the developer's ambient DaggerML environment
