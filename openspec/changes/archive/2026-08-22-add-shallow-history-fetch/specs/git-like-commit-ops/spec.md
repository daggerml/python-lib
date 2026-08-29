## ADDED Requirements

### Requirement: Repository inspection SHALL expose shallow history safely
Repository inspection SHALL resolve and describe locally available commits without network access. Log SHALL return available history and a truncation indicator when traversal reaches shallow history. Show and diff SHALL fail with deepening guidance when an unavailable implicit parent is required, while explicit comparison of two locally available complete snapshots SHALL remain supported.

#### Scenario: Inspect shallow log
- **WHEN** log reaches an intentionally unavailable commit parent before its limit
- **THEN** it returns the available commits with `truncated = true` and performs no network access

#### Scenario: Compare available shallow snapshots explicitly
- **WHEN** diff receives two locally available commits whose complete snapshots exist
- **THEN** it compares those snapshots even if older ancestry is shallow

#### Scenario: Show requires unavailable parent
- **WHEN** show requires the implicit parent of a shallow frontier commit
- **THEN** it fails with deepening guidance and performs no network access

### Requirement: History mutation SHALL require provable ancestry
Merge, rebase, and revert SHALL proceed when all ancestry and merge-base facts required by the operation are proven using locally available commits. If traversal reaches intentionally unavailable history before proving a required fact, the operation SHALL fail with deepening guidance and SHALL NOT interpret the revisions as unrelated histories or mutate refs.

#### Scenario: Fast-forward above shallow boundary
- **WHEN** merge finds the current commit in the available ancestry of the selected commit
- **THEN** it fast-forwards without requiring history older than that common commit

#### Scenario: Unknown merge base
- **WHEN** merge reaches a shallow boundary before finding or disproving a merge base
- **THEN** it fails without creating an empty-base merge commit

#### Scenario: Revert parent is unavailable
- **WHEN** revert requires an intentionally unavailable parent of its target commit
- **THEN** it fails without advancing the current branch

## MODIFIED Requirements

### Requirement: Repository status reports upstream synchronization
The system SHALL report the current branch's configured upstream and ahead/behind counts relative to its local remote-tracking ref when available. It SHALL report counts only when they can be proven from available history; reaching a shallow boundary before proving complete counts SHALL produce unavailable counts rather than partial values.

#### Scenario: Status reports configured upstream
- **WHEN** attached branch `feature` tracks remote branch `main`
- **THEN** status identifies remote branch `main` as its upstream

#### Scenario: Status reports synchronization counts
- **WHEN** the upstream tracking ref exists and available history proves the current branch's complete ahead and behind sets
- **THEN** status reports the computed ahead and behind counts

#### Scenario: Status reports unavailable upstream counts
- **WHEN** the current branch has no upstream or its upstream has not been fetched
- **THEN** status reports unavailable ahead and behind counts without inferring an upstream by local branch name

#### Scenario: Status reaches shallow history
- **WHEN** ahead/behind traversal reaches an intentionally unavailable commit before proving complete counts
- **THEN** status reports ahead and behind as unavailable
