## ADDED Requirements

### Requirement: Tracking refs SHALL become visible after shallow state is valid
Depth-limited fetch SHALL update a local tracking ref only after every included commit snapshot is materialized and every omitted unavailable parent is recorded in valid shallow-history metadata. A failed fetch MAY leave unreferenced immutable objects but SHALL preserve the prior tracking ref and valid shallow metadata.

#### Scenario: Depth fetch fails before completion
- **WHEN** an object required by an included snapshot cannot be fetched or validated
- **THEN** the selected tracking ref retains its prior value
- **AND** no invalid shallow boundary is exposed through that ref

## MODIFIED Requirements

### Requirement: Pull fetches and merges the configured upstream
The system SHALL implement branch pull as fetching the current attached branch's configured upstream branch from `remote.root` followed by merge of that upstream tracking ref into the current branch. Pull SHALL accept an optional positive history depth and no positional remote or branch argument. Pull without depth SHALL fetch new remote commits until it reaches locally available history while preserving any older shallow boundary. Pull SHALL fail without advancing the branch when the fetched history is insufficient to prove the required merge relationship.

#### Scenario: Pull configured upstream
- **WHEN** current local branch `feature` tracks remote-root branch `main` and `dml pull` succeeds
- **THEN** the remote tracking ref for `main` is refreshed and `feature` advances to the merge result or fetched commit when fast-forwardable

#### Scenario: Pull shallow branch incrementally
- **WHEN** a shallow local branch tip is an ancestor of a newer remote upstream tip through remotely available commits
- **THEN** pull materializes the connecting commits, preserves the older shallow boundary, and fast-forwards the local branch

#### Scenario: Pull depth cannot prove ancestry
- **WHEN** pull with a requested depth stops before reaching history needed to prove a merge relationship
- **THEN** pull fails with deepening guidance without advancing the local branch

#### Scenario: Pull untracked branch fails
- **WHEN** the current attached branch has no configured upstream
- **THEN** `dml pull` fails without fetching or advancing the branch

#### Scenario: Pull remote argument is rejected
- **WHEN** a user supplies a positional remote or branch argument to `dml pull`
- **THEN** command parsing rejects the invocation
