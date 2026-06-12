## ADDED Requirements

### Requirement: Commits do not carry a current DAG pointer
The system SHALL model commits as immutable history records containing parent refs, a tree ref, and commit metadata, and SHALL NOT expose a dedicated commit-level current-DAG field.

#### Scenario: Commit description omits current DAG field
- **WHEN** the system describes a commit for history inspection
- **THEN** the description includes commit metadata and the commit tree's DAG map
- **AND** it does not include `commit.dag` or any equivalent commit-level current-DAG pointer

#### Scenario: Unnamed finalized execution DAG stays out of history
- **WHEN** runtime work finalizes an execution DAG without adding a named DAG entry to the commit tree
- **THEN** the finalized DAG is returned as a durable DAG ref
- **AND** no history commit is created for that finalization
- **AND** the finalized DAG is not reintroduced as a dedicated field on any commit object

### Requirement: Runtime commit only advances history for named DAG publication
The system SHALL finalize runtime DAGs independently from history updates. A runtime commit operation SHALL always return the finalized DAG ref and SHALL only create or advance commit history when the finalized DAG is published into the commit tree under a name.

#### Scenario: Named runtime commit creates history and returns finalized DAG
- **WHEN** runtime work finalizes a DAG with `name` set
- **THEN** the operation returns the finalized DAG ref
- **AND** it also creates a commit whose tree records that DAG under the given name

#### Scenario: Unnamed runtime commit does not advance HEAD
- **WHEN** runtime work finalizes a DAG with `name` unset
- **THEN** the operation returns the finalized DAG ref
- **AND** it does not create a commit
- **AND** it does not change `HEAD` or the current branch ref
