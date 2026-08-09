## MODIFIED Requirements

### Requirement: Revision resolution returns canonical commit refs
The DML resolution layer SHALL accept namespace-independent revisions including direct commit refs, commit IDs, `HEAD` ancestry, branch names, and `@tag` with every local, remote, or dependency source selection. A separate normalized source SHALL select symbolic lookup state. Exact commits SHALL resolve from the local object database independently of source. Resolution SHALL return a canonical commit `Ref` without network access or raise when the valid form cannot resolve. Only simultaneous remote and dependency selection is an invalid source combination.

#### Scenario: Resolve a local symbolic revision
- **WHEN** a caller resolves branch `main` with local source
- **THEN** the resolver returns the commit from local branch refs

#### Scenario: Resolve the same revision from remote tracking
- **WHEN** a caller resolves branch `main` with remote source
- **THEN** the resolver returns the commit from remote tracking refs

#### Scenario: Resolve a dependency tag
- **WHEN** a caller resolves `@v1` with dependency source `models`
- **THEN** the resolver returns the commit from dependency `models` tracking tags

#### Scenario: Reject invalid revision or source
- **WHEN** a selector is malformed, remote and dependency arguments conflict, or the valid selector cannot resolve from available local state
- **THEN** the resolver raises `DmlRepoError` without network access
