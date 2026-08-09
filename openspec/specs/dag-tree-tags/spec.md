## Purpose

Allow researchers to classify named committed DAG entries with opaque tags while preserving that classification in repository history.

## Requirements

### Requirement: Tree stores tags for named DAG entries
Each committed tree SHALL store a required mapping from DAG names to lists of string tags in addition to its DAG mapping. Every key in the tag mapping MUST identify a DAG name present in the same tree's DAG mapping. The system SHALL reject tree data with non-string names or tag values, non-list tag values, or tags for absent DAG names.

#### Scenario: Tree stores classification for a DAG
- **WHEN** a tree contains a DAG named `trial-1` with the tag list `["research.v0"]`
- **THEN** the tree retains `research.v0` as the tags for `trial-1`

#### Scenario: Tree rejects a tag for an absent DAG
- **WHEN** tree data assigns tags to `missing` and no DAG named `missing` exists in that tree
- **THEN** the system rejects the tree data

#### Scenario: Legacy tree lacks required tags
- **WHEN** the system reads persisted tree data without a `tags` field
- **THEN** the read fails without supplying a default tag mapping or compatibility conversion

### Requirement: Tags follow tree-entry history
The system SHALL preserve a named DAG entry's tags when tree history operations preserve that entry. Deleting a named DAG entry MUST remove its tags. Replacing or checking out a DAG under a name MUST create an untagged entry at that name unless a subsequent tag mutation adds tags. Concurrent changes to either the DAG reference or tags for the same name MUST be treated as a merge conflict.

#### Scenario: Rebase preserves an unchanged tagged entry
- **WHEN** a commit containing an unchanged tagged DAG entry is replayed during rebase
- **THEN** the rebased tree contains that DAG entry with the same tags

#### Scenario: Deleting a tagged DAG removes its tags
- **WHEN** a user deletes a named DAG entry that has tags
- **THEN** the successor tree contains neither that DAG entry nor its tag mapping entry

#### Scenario: Concurrent tag edits conflict
- **WHEN** two diverged branches produce different tags for the same DAG name
- **THEN** merging the branches reports a conflict for that name

### Requirement: History inspection exposes tree tags
Commit inspection and commit-log entries SHALL include the tree's tags as a mapping from DAG name to list of strings alongside the existing DAG mapping. Tags SHALL be returned exactly as stored; the system SHALL not interpret, validate against a schema, filter, or query them.

#### Scenario: Inspecting a tagged commit
- **WHEN** a user inspects a commit containing tags for `trial-1`
- **THEN** the inspection payload includes those tags under `tags["trial-1"]`

#### Scenario: Inspecting an earlier commit
- **WHEN** a user reads commit history that includes a tag change
- **THEN** each log entry reports the tags stored by that entry's own tree snapshot

### Requirement: Users can add and remove DAG-entry tags
The `Dml.dag` namespace SHALL expose `add_tag(dag: str, tag: str) -> Ref` and `remove_tag(dag: str, tag: str) -> Ref`. Both methods SHALL operate on the current attached branch and create a successor commit when they change the named DAG entry's tags. `add_tag` SHALL append a tag only when it is absent, and `remove_tag` SHALL remove a tag only when it is present. If the requested operation makes no change, the method SHALL return the current commit without creating a successor commit. Both methods SHALL reject a missing DAG name or a detached HEAD.

#### Scenario: Add a new tag
- **WHEN** a user adds `research.v0` to an untagged DAG named `trial-1` on an attached branch
- **THEN** the branch advances to a successor commit whose `tags["trial-1"]` is `["research.v0"]`

#### Scenario: Add an existing tag
- **WHEN** a user adds a tag that is already assigned to the named DAG
- **THEN** the method returns the current commit and leaves the tag list unchanged

#### Scenario: Remove a tag
- **WHEN** a user removes the only tag assigned to `trial-1`
- **THEN** the successor tree has no tag mapping entry for `trial-1`

#### Scenario: Tag mutation has no matching DAG
- **WHEN** a user adds or removes a tag for a DAG name absent from the current tree
- **THEN** the method raises a repository error and does not advance the branch

#### Scenario: Tag mutation on detached HEAD
- **WHEN** a user adds or removes a tag while HEAD is detached
- **THEN** the method raises a repository error and does not create a commit

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
