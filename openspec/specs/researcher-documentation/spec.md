## Purpose
Define the documentation path for researchers using DaggerML workflows.

## Requirements

### Requirement: Researcher documentation SHALL cover the research lifecycle
The documentation SHALL provide a Use DaggerML path for researchers that covers project creation, DAG and funk authoring, execution, results, artifacts, codecs, runtime control, cache control, history, remotes, sharing, reuse, failure inspection, and cleanup at appropriate progressive levels.

#### Scenario: Researcher begins a project
- **WHEN** a new researcher opens the Use DaggerML path
- **THEN** the path provides a concise getting-started workflow from installation and `dml init` through authoring and inspecting a first DAG

#### Scenario: Advanced researcher needs runtime control
- **WHEN** a researcher needs to inspect, cancel, or refresh a running or cached computation
- **THEN** the Use DaggerML path explains the runtime and cache concepts and provides the relevant CLI workflow

### Requirement: Researcher examples SHALL use the CLI for project administration
Researcher-facing guides and onboarding examples SHALL use `dml` for repository initialization, configuration, inspection, history, remote synchronization, runtime administration, cache administration, and cleanup. Python examples SHALL focus on authoring and using research within an already initialized project.

#### Scenario: Guide creates a project
- **WHEN** a researcher guide demonstrates creating a DaggerML project
- **THEN** it uses `dml init` rather than `Dml.init(...)`

#### Scenario: Guide authors a DAG
- **WHEN** a researcher guide demonstrates writing a DAG or funk
- **THEN** its Python example assumes an existing project and uses the Python authoring surface

### Requirement: Researcher documentation SHALL distinguish supported composition from extension implementation
The Use DaggerML path SHALL present Docker image creation, supported execution environments, external artifacts, custom codecs, and temporary DML projects as advanced researcher workflows without requiring adapter, executor, registry, or protocol implementation knowledge.

#### Scenario: Researcher packages a workload
- **WHEN** a researcher needs to run work in Docker or through another supported execution boundary
- **THEN** the docs explain how to compose that supported capability into a DAG and link to extension material only for readers implementing the capability itself

#### Scenario: Researcher uses temporary DML state
- **WHEN** a researcher needs an isolated disposable project
- **THEN** the docs explain the `temporary()` helper as a research authoring convenience and state its lifecycle

### Requirement: Researcher documentation SHALL use research-facing terminology
The Use DaggerML path SHALL use "runtime" as the primary term for an active or inspectable computation and SHALL explain its relationship to a DAG node. It SHALL introduce internal terms such as "index" only when necessary to understand an exact interface or diagnostic.

#### Scenario: Reader learns about active computation
- **WHEN** a researcher reads a runtime guide or CLI reference
- **THEN** the guide describes the user-visible object as a runtime rather than leading with its internal index representation
