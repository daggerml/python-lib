## Purpose

Define bounded local commit-history materialization while preserving complete, usable snapshots and distinguishing intentional shallow boundaries from repository corruption.

## Requirements

### Requirement: Depth SHALL limit only commit ancestry
The system SHALL accept a positive integer history depth for project commit materialization. Depth one SHALL include the selected commit, and each additional depth generation SHALL include every parent of commits in the previous generation. For every included commit, the system SHALL materialize its complete tree, DAG, node, datum, error, and imported-object closure without applying the history depth to those objects. Existing locally available objects SHALL NOT be removed or hidden to enforce a requested depth.

#### Scenario: Depth one produces a usable snapshot
- **WHEN** a caller fetches a branch at depth one
- **THEN** the selected commit and every object reachable through its tree are available locally
- **AND** its otherwise-unavailable parent commits remain absent

#### Scenario: Merge parents count as one generation
- **WHEN** a selected merge commit has two parents and is fetched at depth two
- **THEN** both parent commits and both complete parent snapshots are materialized

#### Scenario: Existing deeper history remains visible
- **WHEN** a repository already contains three generations beneath a selected commit and fetch requests depth one
- **THEN** the existing generations remain available

### Requirement: Shallow history SHALL be explicit local state
The system SHALL record each intentionally unavailable commit ref in repository-local `.dml/shallow.json` using the exact object `{"version": 0, "missing": [...]}`. The version SHALL be a non-boolean integer equal to `0`, and `missing` SHALL be a sorted, unique array containing only exact commit refs. The reader SHALL reject every other version, unknown or missing field, malformed ref, wrong type, duplicate, or unsorted value. The metadata SHALL NOT alter immutable commit, tree, DAG, remote ref, or remote CAS representations. An unavailable commit ref not recorded by this metadata SHALL be treated as repository corruption rather than a valid shallow boundary.

#### Scenario: Omitted parent is recorded
- **WHEN** depth-limited materialization stops before an unavailable commit parent
- **THEN** that exact parent commit ref is recorded in version-0 shallow metadata before the fetched tracking ref becomes visible

#### Scenario: Initial shallow schema is exact
- **WHEN** shallow metadata is written
- **THEN** it contains exactly `version` with non-boolean integer value `0` and `missing` with sorted unique commit refs

#### Scenario: Unsupported shallow shape fails closed
- **WHEN** shallow metadata contains another version, an extra or missing field, or malformed `missing` content
- **THEN** repository access fails with an invalid-shallow-metadata error instead of accepting or migrating it

#### Scenario: Missing non-history object is corruption
- **WHEN** a materialized commit's tree or DAG closure refers to an unavailable object
- **THEN** the operation fails as an incomplete or corrupt repository even if shallow-history metadata exists

#### Scenario: Immutable identity is preserved
- **WHEN** a shallow commit is materialized
- **THEN** its parent list and content-derived commit identity remain identical to the remote object

### Requirement: Fetch SHALL support incremental depth and unshallowing
Depth-limited fetch SHALL reuse locally available objects, materialize missing history needed to satisfy the requested depth from the selected tip, remove shallow entries whose commits become available, and record newly omitted parents. Fetch without depth or unshallow selection SHALL preserve an existing older shallow boundary after connecting new remote history to locally available history. Explicit unshallowing SHALL traverse through locally present frontier commits until all reachable commit ancestry is materialized. Depth and unshallow selection SHALL be mutually exclusive.

#### Scenario: Increase available depth
- **WHEN** a branch fetched at depth one is fetched again at depth three
- **THEN** its next two parent generations and complete snapshots become available
- **AND** shallow metadata moves to any still-unavailable parent frontier

#### Scenario: Unshallow selected history
- **WHEN** explicit unshallowing is requested for a shallow branch
- **THEN** every reachable parent commit is materialized and no shallow entry remains for that history

#### Scenario: Ordinary update preserves old boundary
- **WHEN** a shallow branch gains two remote commits and is fetched without depth or unshallow selection
- **THEN** the two new commits are materialized through the existing local tip
- **AND** the older shallow boundary remains

#### Scenario: Reject conflicting materialization options
- **WHEN** a caller supplies both depth and unshallow selection
- **THEN** the request fails before changing objects, shallow metadata, or tracking refs

### Requirement: History traversal SHALL report shallow termination
History traversal SHALL stop cleanly at intentionally unavailable commits and SHALL distinguish that result from reaching a root commit. Log results SHALL indicate whether requested traversal was truncated by shallow history. First-parent revision traversal and implicit-parent comparison SHALL fail with deepening guidance when their required commit is intentionally unavailable.

#### Scenario: Log reports truncation
- **WHEN** log reaches an intentionally unavailable parent before satisfying its requested limit
- **THEN** it returns the available commits and identifies the result as truncated

#### Scenario: HEAD ancestry crosses a shallow boundary
- **WHEN** `HEAD~N` requires an intentionally unavailable commit
- **THEN** revision resolution fails with guidance to deepen or unshallow the selected history

#### Scenario: Implicit diff parent is unavailable
- **WHEN** show or diff requires a selected commit's intentionally unavailable parent
- **THEN** it fails with deepening guidance rather than treating the selected commit as a root

### Requirement: Ancestry-dependent operations SHALL fail on unknown history
Ancestry and merge-base evaluation SHALL distinguish a proven negative result from a traversal stopped by shallow history. Status SHALL report unavailable ahead/behind counts, and merge, rebase, revert, and publication safety checks SHALL fail without mutation when their required relationship cannot be proven because history is shallow.

#### Scenario: Merge base is below the shallow boundary
- **WHEN** two revisions may share a merge base only through intentionally unavailable commits
- **THEN** merge fails with deepening guidance rather than merging them as unrelated histories

#### Scenario: Fast-forward is proven above the boundary
- **WHEN** the destination commit is found in the available ancestry of the source before any shallow boundary
- **THEN** the system may perform the fast-forward operation

#### Scenario: Status cannot complete ancestry sets
- **WHEN** ahead/behind calculation reaches shallow history before proving complete counts
- **THEN** status reports the counts as unavailable

### Requirement: Local garbage collection SHALL preserve valid shallow repositories
Local garbage collection SHALL treat recorded unavailable commit refs as terminal history leaves while continuing to require every other traversed ref to exist. It SHALL remove stale shallow entries whose refs are neither absent ancestry of a retained object nor otherwise relevant to retained local state.

#### Scenario: Collect shallow repository
- **WHEN** local garbage collection traces a retained commit whose parent is recorded as intentionally unavailable
- **THEN** collection completes without treating that parent as a missing-object failure
- **AND** preserves every available object reachable from retained roots

#### Scenario: Undeclared missing object still fails
- **WHEN** local garbage collection encounters an unavailable ref not declared by shallow-history metadata
- **THEN** collection fails rather than silently accepting repository corruption
