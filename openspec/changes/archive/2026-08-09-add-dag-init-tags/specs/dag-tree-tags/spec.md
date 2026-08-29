## ADDED Requirements

### Requirement: Public DAG wrappers can assign tags on commit
The public DAG wrapper SHALL accept an optional list of string tags that defaults to no tags. After successfully committing a named DAG, the wrapper SHALL add each provided tag to the committed DAG's tree entry using the existing tag mutation semantics. When tags are omitted or the provided list is empty, committing the DAG SHALL perform no tag mutations.

#### Scenario: Commit with tags
- **WHEN** an author initializes a named DAG wrapper with `tags=["research.v0", "candidate"]` and successfully commits it
- **THEN** the resulting tree entry for that DAG has both tags in the provided order

#### Scenario: Commit without tags
- **WHEN** an author initializes a DAG wrapper without tags and commits it
- **THEN** the commit flow performs no tag mutations

#### Scenario: Commit with an empty tag list
- **WHEN** an author initializes a DAG wrapper with an empty tag list and commits it
- **THEN** the commit flow performs no tag mutations

#### Scenario: DAG commit fails
- **WHEN** the underlying DAG commit fails before publishing the named DAG entry
- **THEN** the wrapper performs no tag mutations

#### Scenario: Tag mutation fails after DAG commit
- **WHEN** the DAG commit succeeds and adding a provided tag fails
- **THEN** the tag mutation error propagates to the author
- **AND** the already successful DAG commit remains published
