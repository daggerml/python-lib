## MODIFIED Requirements

### Requirement: Dml clone bootstraps from a remote root
The system SHALL expose `Dml.clone(revision: Ref | str | None = None, /, *, project_home: str = ".", remote_root: str | None = None, depth: int | None = None, ...)`. It SHALL resolve the project endpoint exclusively through normal `remote.root` configuration precedence, require a valid resolved root, initialize the local repository, persist that resolved root, materialize the optional revision from the endpoint at the requested positive commit-history depth, and set HEAD to the resolved commit. Omitting depth SHALL materialize complete history. Branch revisions SHALL create an attached same-named local branch and upstream; tags, ancestry, and exact commits SHALL leave HEAD detached. Any accepted revision that cannot be materialized SHALL fail clone.

#### Scenario: Clone selected branch
- **WHEN** resolved configuration supplies `remote.root = "s3://bucket/demo"` and clone receives revision `feature`
- **THEN** the system persists the resolved root, fetches branch `feature`, leaves HEAD attached to local branch `feature`, and records upstream branch `feature`

#### Scenario: Clone selected branch at depth one
- **WHEN** clone receives branch `feature` and depth one
- **THEN** it materializes the feature tip and complete current snapshot, records unavailable commit parents as shallow history, and leaves HEAD attached to local branch `feature`

#### Scenario: Clone selected tag
- **WHEN** resolved configuration supplies `remote.root = "s3://bucket/demo"` and clone receives revision `@v1`
- **THEN** the system persists the resolved root, fetches tag `v1`, and leaves HEAD detached at the fetched commit

#### Scenario: Clone selected exact commit
- **WHEN** clone receives an exact commit available from resolved `remote.root`
- **THEN** it materializes that commit at the requested depth and leaves HEAD detached at that commit

#### Scenario: Reject invalid clone depth
- **WHEN** clone receives a non-positive depth
- **THEN** it fails without presenting an initialized checkout as successful

#### Scenario: Clone unresolvable revision fails
- **WHEN** the supplied revision is accepted by the grammar but cannot be materialized from resolved `remote.root`
- **THEN** clone fails without presenting an initialized checkout as successful
