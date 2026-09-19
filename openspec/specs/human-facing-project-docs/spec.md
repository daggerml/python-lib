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
Primary navigation SHALL contain Start here, Use, and Extend sections. Glossary and Sharp bits and security SHALL be direct top-level siblings of those sections. It SHALL NOT contain separate Concepts, Guides, Reference, Examples, or Develop sections.

#### Scenario: Reader looks for onboarding
- **WHEN** a new researcher wants the fastest path to first success
- **THEN** the docs home directs them into the ordered Start here course

#### Scenario: Reader looks for detailed information
- **WHEN** a reader needs to use DaggerML or implement an integration
- **THEN** the docs navigation directs them to Use or Extend without first requiring them to classify the page as a concept, guide, reference, or example

### Requirement: Documentation source paths SHALL mirror published paths
Human-facing QMD source paths SHALL match their published HTML paths. Start here pages SHALL live directly beneath `docs/start-here/`, researcher pages directly beneath `docs/use/`, and extension pages directly beneath `docs/extend/`. Directory landing pages SHALL use `index.qmd`. Glossary and Sharp bits and security SHALL remain top-level QMD siblings. A separate documentation examples source or route SHALL NOT exist.

#### Scenario: Maintainer locates a published page
- **WHEN** a maintainer maps a `/docs/<section>/<page>` route back to its QMD source
- **THEN** the corresponding page is found at `docs/<section>/<page>.qmd`

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

### Requirement: Human-facing docs SHALL avoid normative spec voice
Docs under `docs/` SHALL describe the system in reader-facing language and SHALL avoid structuring pages around authority ownership, compatibility classifications, or normative maintenance phrases such as document-level handoff rules.

#### Scenario: Reader opens a topic doc
- **WHEN** a reader opens a concept, guide, reference, or architecture page under `docs/`
- **THEN** the document leads with explanation of the subject matter instead of an authority or governance preamble

### Requirement: Existing technical content SHALL be preserved through translation, not path churn
When current docs are flattened, the implementation SHALL preserve useful reader-facing technical knowledge in an appropriate Use or Extend page rather than deleting depth solely because its former concept, guide, reference, example, or Develop category is removed. Contributor-only orientation SHALL move to the repository README nearest the code it describes.

#### Scenario: Existing detailed doc is migrated
- **WHEN** a current technical document contains valuable behavioral or architectural explanation
- **THEN** the flat documentation preserves that information in Use or Extend even if the page name and presentation change

### Requirement: Maintainer workflow docs SHALL leave `docs/`
Repository-maintenance documents, contributor setup and testing policy, codebase orientation, architecture notes, edit maps, agent instructions, and spec-governance indexes SHALL live outside the human-facing `docs/` tree. Repository-wide contributor guidance SHALL live in root maintainer files, subsystem orientation SHALL live in co-located README files, and normative behavior SHALL remain in OpenSpec.

#### Scenario: Contributor needs workflow guidance
- **WHEN** a contributor needs setup, testing, edit workflow, agent, or spec-governance guidance
- **THEN** that guidance is available from repository-level maintainer files outside `docs/`

#### Scenario: Contributor needs subsystem orientation
- **WHEN** a contributor needs implementation architecture or a codebase map
- **THEN** they can find it in a README co-located with the relevant source area

### Requirement: Human-facing documentation SHALL be the example surface
Reader-facing examples SHALL appear in the Start here, Use, or Extend page that teaches the behavior. Runnable examples SHALL execute during the documentation build and fail the build on unexpected errors. The documentation SHALL NOT maintain a separate Examples section, parallel canonical example source, or example-only download inventory.

#### Scenario: Reader follows documented behavior
- **WHEN** a page demonstrates a runnable DaggerML workflow
- **THEN** the demonstrated code is part of that page and has been verified by the documentation build

#### Scenario: Maintainer changes an example
- **WHEN** a maintainer changes runnable code shown in the documentation
- **THEN** there is no separate reader-facing example copy that must be updated in parallel

### Requirement: Documentation build entrypoint SHALL live with documentation
The repository SHALL provide `docs/build.sh` as the single local, CI, and release entrypoint for producing verified executable documentation and packaged dashboard assets. Given host-provided Python, Node/npm, `uv`, source-control and download utilities, and native build prerequisites, the default invocation SHALL synchronize project Python dependencies, install locked frontend dependencies, bootstrap the pinned documentation toolchain, run frontend tests, execute and stage the documentation, compile the frontend, assemble the combined static tree, and validate the packaged result. Dashboard and release automation SHALL invoke this entrypoint rather than duplicate repository dependency-installation, frontend-test, documentation-render, or frontend-build commands. A duplicate root-level dashboard build entrypoint SHALL NOT remain.

#### Scenario: Maintainer performs the complete build
- **WHEN** a maintainer invokes `bash docs/build.sh` on a host with the documented prerequisite tools
- **THEN** the command prepares repository dependencies and produces a verified packaged dashboard containing the current frontend and executable documentation

#### Scenario: CI or release automation builds the dashboard
- **WHEN** automation has provisioned the documented host prerequisites
- **THEN** it invokes `docs/build.sh` as the sole repository command for dashboard dependency setup, verification, rendering, compilation, and packaging

#### Scenario: A host prerequisite is unavailable
- **WHEN** a required host-provided tool is unavailable
- **THEN** the build fails before consuming incomplete generated output and identifies the missing prerequisite

### Requirement: Dashboard build stages SHALL be composable
The build entrypoint SHALL expose independent options to disable Python dependency synchronization, frontend dependency installation, frontend tests, documentation output, and frontend output. Automatic-versus-forced output rebuilding SHALL remain independent of those stage selections. A disabled setup stage SHALL use existing repository state, a disabled output stage SHALL preserve the corresponding packaged component, and the command SHALL fail clearly when selected downstream work lacks required dependencies or a complete preserved component.

#### Scenario: Contributor performs a fast local rebuild
- **WHEN** a contributor invokes `bash docs/build.sh --no-python-sync --no-npm-ci` with usable dependency environments
- **THEN** the build skips dependency synchronization while retaining verification and both output stages

#### Scenario: Contributor builds documentation only
- **WHEN** a contributor invokes `bash docs/build.sh --no-npm-ci --no-ui-test --no-ui`
- **THEN** the build prepares and renders documentation while preserving the existing packaged frontend

#### Scenario: Contributor builds the frontend only
- **WHEN** a contributor invokes `bash docs/build.sh --no-python-sync --no-docs`
- **THEN** the build installs and verifies frontend dependencies, compiles the frontend, and preserves the existing packaged documentation

#### Scenario: Contributor forces selected outputs
- **WHEN** a contributor combines forced rebuilding with one or more disabled stages
- **THEN** the build forces only the selected output stages and does not re-enable a disabled stage

### Requirement: Dashboard packaging SHALL be transactional
The build SHALL prepare selected documentation and frontend output away from the installed package tree, combine selected output with preserved unselected components, and validate the complete candidate before replacing `src/daggerml/dashboard/static/`. A failed setup, test, render, compilation, assembly, or validation stage SHALL NOT leave a partially replaced packaged dashboard.

#### Scenario: Complete build succeeds
- **WHEN** every selected stage and final validation succeeds
- **THEN** the packaged static tree is replaced with the validated candidate containing both dashboard components

#### Scenario: Selected stage fails
- **WHEN** any selected stage fails
- **THEN** the command exits unsuccessfully and the previously packaged static tree remains intact

#### Scenario: Partial build lacks a preserved component
- **WHEN** documentation or frontend output is disabled and no complete packaged version of that component exists
- **THEN** the command fails before replacing the packaged static tree and identifies the missing preserved component

### Requirement: Build help SHALL document the complete interface
`docs/build.sh --help` SHALL describe host prerequisites, the default complete build, every setup, verification, output-selection, and rebuild-policy option, interactions between disabled and downstream stages, and the generated-output replacement behavior. The help SHALL include comment-labeled examples for the complete clean CI/release build, a fast local rebuild with existing dependency environments, a documentation-only build, and a frontend-only build that preserves packaged documentation.

#### Scenario: Contributor requests build help
- **WHEN** a contributor invokes `bash docs/build.sh --help`
- **THEN** the command exits successfully without installing dependencies or changing generated output and prints the full interface explanation

#### Scenario: Contributor reads build examples
- **WHEN** the help examples are displayed
- **THEN** they include the commands and comment labels `# Complete clean CI/release build` with `bash docs/build.sh`, `# Fast local rebuild with existing dependency environments` with `bash docs/build.sh --no-python-sync --no-npm-ci`, `# Documentation only` with `bash docs/build.sh --no-npm-ci --no-ui-test --no-ui`, and `# Frontend only, preserving packaged docs` with `bash docs/build.sh --no-python-sync --no-docs`

### Requirement: Executable docs SHALL not use ambient user state
The executable course SHALL run in a fresh build workspace with isolated DaggerML and cloud configuration. A page with `dml-project-home` SHALL run from that workspace-relative directory only after a declared dependency has created it.

#### Scenario: A developer builds the documentation
- **WHEN** the complete documentation build succeeds or fails
- **THEN** its repositories, credentials, configuration, services, and temporary state are removed without modifying the developer's ambient DaggerML environment
