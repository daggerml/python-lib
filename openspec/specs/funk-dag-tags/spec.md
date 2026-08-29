## Purpose

Allow script funk definitions to declare durable tags for the DAG produced by each execution.

## Requirements

### Requirement: Script funks declare result-DAG tags
Script-funk authoring SHALL accept an optional list of tags that identifies the DAG produced by each execution. The system SHALL normalize the supplied tags to unique lexicographically sorted strings before creating the execution DAG.

#### Scenario: Funk invocation publishes declared tags
- **WHEN** a script funk declared with tags `["candidate", "research.v0", "candidate"]` executes successfully
- **THEN** its published result DAG has tags `["candidate", "research.v0"]`

#### Scenario: Funk failure retains declared tags
- **WHEN** a script funk declared with tags raises while executing
- **THEN** its published error DAG retains the declared normalized tags

#### Scenario: Cached result retains declared tags
- **WHEN** a later invocation reuses a cached result of a tagged script funk
- **THEN** the returned result DAG exposes the tags persisted by the executed result
