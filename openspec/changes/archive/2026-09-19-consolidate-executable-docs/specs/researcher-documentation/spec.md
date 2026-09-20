## MODIFIED Requirements

### Requirement: Researcher documentation SHALL cover the research lifecycle
The documentation SHALL provide an ordered Use course containing exactly six primary pages named Projects, Artifacts, Execution, Inspection, Runtimes, and Sharing. The course SHALL continue from the completed Start here project and cover project orientation and configuration; durable DAG data, external artifacts, and installed-codec selection; supported execution boundaries; result, provenance, runnable, artifact, failure, and committed-DAG `Projection` inspection; runtime and cache administration; and history, remotes, publication, and reuse. Inspection SHALL own user-facing `Projection` traversal, value/context, and reuse: Python item access against a committed immutable DAG returns a `Projection` when it cannot append the `get` node that open-DAG item access would create. Artifacts SHALL own durable values, `Uri`, `S3Store`, and external payloads. The pages SHALL combine explanation, exact interfaces, runnable workflows, failure guidance, and relevant reference detail without separate concept, guide, or reference pages.

#### Scenario: Researcher continues after onboarding
- **WHEN** a researcher completes the Start here course and opens the Use path
- **THEN** Projects begins from the project produced by Start here and the remaining pages form a declared sequence through Artifacts, Execution, Inspection, Runtimes, and Sharing

#### Scenario: Researcher follows the complete lifecycle
- **WHEN** a researcher follows the Use course in order
- **THEN** each page consumes durable project or fixture state produced by its prerequisites and leaves the state needed by later pages

#### Scenario: Researcher traverses a committed collection
- **WHEN** Python item access targets a committed immutable DAG and cannot append an open-DAG `get` node
- **THEN** Inspection explains and demonstrates the returned `Projection`, including its traversal, value/context, and reuse behavior

#### Scenario: Researcher needs exact interface detail
- **WHEN** a workflow requires CLI, Python, configuration, runtime-state, or error details
- **THEN** the relevant course page provides that detail in context instead of sending the reader to a duplicate standalone reference page

#### Scenario: Advanced researcher controls work
- **WHEN** a researcher needs to inspect, cancel, or refresh a running or cached computation
- **THEN** the Runtimes page explains the behavior, demonstrates fixture-owned operations, and states shared-remote consequences

### Requirement: Researcher examples SHALL use the CLI for project administration
Researcher-facing pages SHALL use executable `dml` examples for repository initialization, configuration, inspection, history, remote synchronization, runtime administration, cache administration, and cleanup. Executable Python examples SHALL focus on authoring, materializing, and inspecting research within an initialized project. The primary Use journey SHALL execute during the documentation build with assertions against build-owned projects, object storage, and remotes; examples requiring infrastructure not owned by the build SHALL be explicitly identified as pseudocode rather than presented as verified workflows. Examples SHALL live in the page that teaches the workflow rather than in a separate examples tree.

#### Scenario: Use page administers a project
- **WHEN** a Use workflow configures, inspects, publishes, or controls a DaggerML project
- **THEN** it executes the applicable `dml` commands against documentation-owned state and verifies their observable results

#### Scenario: Use page authors or inspects research
- **WHEN** a Use workflow creates or traverses DAG data, artifacts, funks, or provenance
- **THEN** its executable Python runs inside the declared project and verifies values produced by that course journey

#### Scenario: Workflow needs external infrastructure
- **WHEN** an execution example requires an SSH host, scheduler, or cloud compute service not owned by the documentation fixture
- **THEN** the page marks that example as pseudocode, identifies the prerequisite, and keeps a fixture-owned local or Docker workflow executable

#### Scenario: Researcher repeats a documented workflow
- **WHEN** a reader follows runnable commands from a Use page
- **THEN** those commands are the same examples verified by the documentation build

### Requirement: Researcher documentation SHALL distinguish supported composition from extension implementation
The Use course SHALL present supported execution environments, external artifacts, built-in codec behavior, temporary DaggerML projects, dashboards, and other researcher-facing composition without requiring adapter, executor, registry, protocol, or plugin-provider implementation knowledge. Custom codec and dashboard-provider implementation SHALL live in extension or subsystem documentation, with Use retaining only the guidance needed to select and operate installed capabilities.

#### Scenario: Researcher packages a workload
- **WHEN** a researcher needs to run work in Docker or through another supported execution boundary
- **THEN** the Execution page explains how to compose that capability and links to Extend only for readers implementing the integration

#### Scenario: Researcher encounters a custom value
- **WHEN** a value requires a codec not supplied by the installed environment
- **THEN** the Artifacts page explains the need and directs codec implementers to Extend without reproducing the codec contract

#### Scenario: Researcher uses temporary DaggerML state
- **WHEN** a researcher needs an isolated disposable project
- **THEN** the Projects page demonstrates the temporary-project lifecycle as a research convenience

#### Scenario: Researcher uses a dashboard provider
- **WHEN** a researcher selects or operates an installed custom DAG dashboard
- **THEN** Use explains the user workflow without embedding provider registration and result-schema contracts
