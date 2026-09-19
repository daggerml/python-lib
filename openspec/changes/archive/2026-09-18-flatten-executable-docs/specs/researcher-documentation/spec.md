## MODIFIED Requirements

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
