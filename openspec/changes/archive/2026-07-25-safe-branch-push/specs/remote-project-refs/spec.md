## MODIFIED Requirements

### Requirement: Branch heads are mutable and project tags are immutable
The system SHALL allow project branch head refs to move through safe update operations. The system SHALL reject a non-forced attempt to overwrite an existing project tag ref and SHALL allow a forced attempt to replace it.

#### Scenario: Branch head update
- **WHEN** a push safely advances project `alice/demo` branch `main`
- **THEN** the existing `refs/projects/alice/demo/heads/main.json` ref may be replaced by the new branch head payload

#### Scenario: Tag overwrite rejected
- **WHEN** `refs/projects/alice/demo/tags/v1.0.json` already exists
- **THEN** publishing tag `v1.0` fails without changing the existing tag ref

#### Scenario: Forced tag overwrite succeeds
- **WHEN** `refs/projects/alice/demo/tags/v1.0.json` already exists and push requests force
- **THEN** publishing tag `v1.0` replaces the existing tag ref with the requested commit

### Requirement: Push uses conditional publication and fast-forward safety
The system SHALL expose a keyword-only `force` option on `Dml.push()` that defaults to `False`. For a non-forced branch push, the system SHALL read and materialize an existing remote branch tip without modifying local heads or working state, require that tip to be an ancestor of the candidate commit, and conditionally replace the branch ref using the observed ETag. If the remote branch is absent, the system SHALL create it only if it remains absent. A forced branch or tag push SHALL overwrite the ref without reading, ancestry validation, or conditional-write checks.

#### Scenario: Missing branch is created safely
- **WHEN** a non-forced push targets a remote branch ref that does not exist
- **THEN** push creates the branch ref only if it still does not exist

#### Scenario: Missing branch creation loses race
- **WHEN** a non-forced push observes that a remote branch ref is absent and another client creates it before publication
- **THEN** push fails without overwriting the remote branch ref

#### Scenario: Remote branch tip is materialized for validation
- **WHEN** a non-forced push targets an existing remote branch ref
- **THEN** the system materializes the remote commit closure locally for ancestry validation without updating local tracking refs, branch refs, or HEAD

#### Scenario: Fast-forward push
- **WHEN** the remote branch head is an ancestor of the local branch head and the observed ETag still matches
- **THEN** push updates the remote branch head to the local commit

#### Scenario: Non-fast-forward push rejected
- **WHEN** the remote branch head is not an ancestor of the local branch head and force is not requested
- **THEN** push fails without updating the remote branch head

#### Scenario: Conditional update loses race
- **WHEN** a non-forced push validates a remote branch head and another client updates that branch before publication
- **THEN** push fails without overwriting the newer remote branch head

#### Scenario: Force push overwrites a ref
- **WHEN** force is requested for a branch or tag push
- **THEN** push overwrites the remote ref with the local commit without remote-tip validation or conditional-write checks
