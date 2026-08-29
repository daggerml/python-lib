## Purpose

Allow researchers to classify immutable DAGs with durable opaque tags.

## Requirements

### Requirement: DAGs store normalized intrinsic tags
Each DAG SHALL store a required list of opaque string tags. The list SHALL contain no duplicates and SHALL be sorted in lexicographic order. Tags SHALL be part of the DAG's immutable persisted content and SHALL remain associated with that DAG when it is published as an execution result, imported, checked out, or referenced under another name.

#### Scenario: DAG stores normalized tags
- **WHEN** a DAG is created with tags `["candidate", "research.v0", "candidate"]`
- **THEN** its persisted tags are `["candidate", "research.v0"]`

#### Scenario: DAG result retains tags
- **WHEN** an execution publishes a DAG with the tag `research.v0`
- **THEN** a consumer resolving that result DAG observes `research.v0` in the DAG's tags without requiring a named tree entry

### Requirement: Active runtimes can mutate DAG tags
The runtime SHALL expose add-tag and remove-tag operations for active indexes. Adding an existing tag or removing an absent tag SHALL leave the active DAG tags unchanged. Tag mutation SHALL reject frozen indexes and completed DAG refs.

#### Scenario: Add and remove tags on an active index
- **WHEN** a user adds `candidate` and removes `research.v0` on an active index whose tags are `["research.v0"]`
- **THEN** its active DAG tags are `["candidate"]`

#### Scenario: Tag mutation rejects a frozen index
- **WHEN** a user attempts to add or remove a tag on a frozen index
- **THEN** the operation raises an error and leaves the frozen DAG unchanged

### Requirement: Public DAG wrappers expose persisted tags
Public DAG wrappers for both active and loaded DAGs SHALL expose the tags of their underlying DAG as a normalized list of strings. Creating a public DAG with tags SHALL initialize the active DAG with those tags, and committing the wrapper SHALL persist them atomically with its result.

#### Scenario: Live and loaded wrappers agree on tags
- **WHEN** a live DAG created with tags is committed and later loaded
- **THEN** the live and loaded wrappers expose the same normalized tag list

#### Scenario: Failed DAG retains initialized tags
- **WHEN** an active DAG initialized with tags commits an error result
- **THEN** the persisted error DAG retains those tags
