## ADDED Requirements

### Requirement: Dml clone bootstraps from a remote root
The system SHALL expose `Dml.clone(revision: Ref | str | None = None, /, *, project_home: str = ".", remote_root: str | None = None, ...)`. It SHALL resolve the project endpoint exclusively through normal `remote.root` configuration precedence, require a valid resolved root, initialize the local repository, persist that resolved root, materialize the optional revision from the endpoint, and set HEAD to the resolved commit. Branch revisions SHALL create an attached same-named local branch and upstream; tags, ancestry, and exact commits SHALL leave HEAD detached. Any accepted revision that cannot be materialized SHALL fail clone.

#### Scenario: Clone selected branch
- **WHEN** resolved configuration supplies `remote.root = "s3://bucket/demo"` and clone receives revision `feature`
- **THEN** the system persists the resolved root, fetches branch `feature`, leaves HEAD attached to local branch `feature`, and records upstream branch `feature`

#### Scenario: Clone selected tag
- **WHEN** resolved configuration supplies `remote.root = "s3://bucket/demo"` and clone receives revision `@v1`
- **THEN** the system persists the resolved root, fetches tag `v1`, and leaves HEAD detached at the fetched commit

#### Scenario: Clone selected exact commit
- **WHEN** clone receives an exact commit available from resolved `remote.root`
- **THEN** it materializes that commit closure and leaves HEAD detached at that commit

#### Scenario: Clone unresolvable revision fails
- **WHEN** the supplied revision is accepted by the grammar but cannot be materialized from resolved `remote.root`
- **THEN** clone fails without presenting an initialized checkout as successful

### Requirement: Clone without a selector uses the default branch
The system SHALL treat clone without a revision as a request for branch `default.branch_name` from resolved `remote.root`.

#### Scenario: Clone bare endpoint root
- **WHEN** clone resolves `remote.root` and receives no revision
- **THEN** the system fetches branch `default.branch_name`, leaves HEAD attached to the corresponding local branch, and records that branch name as its upstream

## REMOVED Requirements

### Requirement: Dml clone bootstraps a local repo from a remote project ref
**Reason**: Clone resolves `remote.root` from shared configuration rather than accepting a project URI or named remote.
**Migration**: Configure `remote.root` through the shared configuration surface and pass only an optional revision to clone.

### Requirement: Bare project clone imputes the default branch
**Reason**: Omitting clone revision selects the default branch from configured `remote.root`; no project URI input exists.
**Migration**: Configure `remote.root` and omit revision to fetch `default.branch_name`.
