## MODIFIED Requirements

### Requirement: HeadOps owns branch and index pointer persistence
The system SHALL route all branch, tag, and index pointer creation, lookup, update, listing, and deletion through `HeadOps` public methods using filesystem refs.

#### Scenario: Non-HeadOps caller needs branch commit
- **WHEN** an internal caller needs the commit for a branch
- **THEN** it obtains that commit through a `HeadOps` public method backed by `.dml/refs` files

#### Scenario: Non-HeadOps caller needs index commit
- **WHEN** an internal caller needs the commit for an index
- **THEN** it obtains that commit through a `HeadOps` public method backed by `.dml/refs` files

### Requirement: HeadOps hides head and index refs from callers
The system SHALL keep pointer file-path and pointer-ref details internal to `HeadOps` and SHALL expose branch names, opaque index ids, and commit refs to non-`HeadOps` callers.

#### Scenario: Callers do not use `head:` or `index:` string forms
- **WHEN** internal or CLI callers target branches or indexes
- **THEN** they use plain branch names and opaque index ids, not `head:<name>` or `index:<id>` strings

### Requirement: HeadOps supports atomic commit updates for pointers
The system SHALL update branch and index commits through `update_branch_commit` and `update_index_commit` methods that require the caller to provide the expected current commit.

#### Scenario: Expected commit matches
- **WHEN** a caller requests a branch or index commit update with the correct current commit
- **THEN** `HeadOps` stores the new commit by atomically replacing the pointer file

#### Scenario: Expected commit is stale
- **WHEN** a caller requests a branch or index commit update with an outdated current commit
- **THEN** `HeadOps` rejects the update and raises a dedicated conflict error

### Requirement: Conflict error reports current commit for retries
The system SHALL raise a dedicated `DmlRepoError` subclass for stale branch/index updates, and that exception SHALL expose the correct `current_commit`.

### Requirement: Pointer roots are commit refs
The system SHALL return commit refs directly from `HeadOps.list_pointer_roots`.

#### Scenario: GC root collection
- **WHEN** callers request pointer roots for reachability traversal
- **THEN** `HeadOps.list_pointer_roots` returns commit refs gathered from all local heads and indexes

### Requirement: GC root traversal remains current-ref based
The system SHALL keep garbage-collection root discovery based on current refs, and SHALL NOT require this change to introduce user-specified GC root arguments.
