## Purpose
Define the documentation path for researchers using DaggerML workflows.

## Requirements

### Requirement: Researcher documentation SHALL cover the research lifecycle
The documentation SHALL provide a flat Use path whose pages cover project creation, DAG and funk authoring, execution, results, artifacts, codecs, runtime control, cache control, history, remotes, sharing, reuse, failure inspection, and cleanup. Pages MAY vary in explanatory depth without being separated into concept, guide, or reference directories.

#### Scenario: Researcher begins a project
- **WHEN** a new researcher opens the Use path
- **THEN** the path connects the unchanged Start here course to directly addressable pages for later research tasks

#### Scenario: Advanced researcher needs runtime control
- **WHEN** a researcher needs to inspect, cancel, or refresh a running or cached computation
- **THEN** a Use page explains the behavior and demonstrates the relevant CLI workflow

### Requirement: Researcher examples SHALL use the CLI for project administration
Researcher-facing pages SHALL use executable `dml` examples for repository initialization, configuration, inspection, history, remote synchronization, runtime administration, cache administration, and cleanup. Executable Python examples SHALL focus on authoring and using research within an initialized project. These examples SHALL live in the page that teaches the workflow rather than in a separate examples tree.

#### Scenario: Use page creates a project
- **WHEN** a researcher page demonstrates creating a DaggerML project
- **THEN** it executes `dml init` rather than `Dml.init(...)`

#### Scenario: Use page authors a DAG
- **WHEN** a researcher page demonstrates writing a DAG or funk
- **THEN** its Python example assumes an existing project and uses the Python authoring surface

#### Scenario: Researcher repeats a documented workflow
- **WHEN** a reader follows runnable commands from a Use page
- **THEN** those commands are the examples verified by the documentation build

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
