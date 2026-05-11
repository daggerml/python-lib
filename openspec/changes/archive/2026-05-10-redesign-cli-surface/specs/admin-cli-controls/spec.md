## ADDED Requirements

### Requirement: Administrative CLI flows are grouped under `dml admin`
Low-frequency maintenance and recovery commands SHALL be exposed under `dml admin` rather than as top-level porcelain commands.

#### Scenario: Admin help groups maintenance commands
- **WHEN** a user inspects `dml admin` help
- **THEN** index management, cache invalidation, remote discovery, remote garbage collection, and local garbage collection appear under `dml admin`

### Requirement: Admin index list returns indexes with commit info
`dml admin index list` SHALL return every live index together with commit information for the commit each index currently points to.

#### Scenario: Index list includes commit summaries
- **WHEN** a user runs `dml admin index list`
- **THEN** the command returns JSON with an `indexes` field
- **AND** each index entry includes its identifier and commit information for the pointed-to commit

### Requirement: Admin index get returns full index inspection payload
`dml admin index get <index-id>` SHALL return index inspection data including commit information for the commit the index points to, rather than only a commit identifier.

#### Scenario: Index get includes commit details
- **WHEN** a user runs `dml admin index get idx1`
- **THEN** the command returns JSON with an `index` object
- **AND** that object includes commit metadata for the pointed-to commit

### Requirement: Admin index delete removes an index
`dml admin index delete <index-id>` SHALL delete the selected index and report the deletion result as JSON.

#### Scenario: Index delete reports success
- **WHEN** a user runs `dml admin index delete idx1`
- **THEN** the command returns JSON containing `index` and `deleted`

### Requirement: Admin cache invalidation accepts exact cache keys only
`dml admin cache invalidate <cache-key> [more cache keys]` SHALL accept one or more exact cache keys and SHALL NOT accept DAG refs, argv refs, or other selector types.

#### Scenario: Cache invalidation accepts multiple exact keys
- **WHEN** a user runs `dml admin cache invalidate ck1 ck2`
- **THEN** the command invalidates those exact cache keys
- **AND** returns JSON containing `cache_keys` and `invalidated`

#### Scenario: Cache invalidation rejects non-key selector forms
- **WHEN** a user runs `dml admin cache invalidate dag:abc123`
- **THEN** the command fails because admin cache invalidation accepts exact cache keys only

### Requirement: Admin remote list can list projects or one project's refs
`dml admin remote list` SHALL support two modes through one command shape.

Without a project argument, it SHALL list remote projects as canonical `dml://<owner>/<project>` URIs and MAY filter by owner. With a `dml://<owner>/<project>` argument, it SHALL list the remote branches and tags for that project.

#### Scenario: Remote list returns projects
- **WHEN** a user runs `dml admin remote list`
- **THEN** the command returns JSON with a `projects` field containing canonical project URIs

#### Scenario: Remote list filters by owner
- **WHEN** a user runs `dml admin remote list --owner alice`
- **THEN** the command returns only projects owned by `alice`

#### Scenario: Remote list returns project refs
- **WHEN** a user runs `dml admin remote list dml://alice/demo`
- **THEN** the command returns JSON containing `project`, `branches`, and `tags`

### Requirement: Admin remote GC performs remote maintenance
`dml admin remote gc` SHALL perform remote maintenance for the configured remote, including remote GC of CAS/refs state and remote transport cleanup, and SHALL report the result as JSON.

#### Scenario: Remote GC reports cleanup summary
- **WHEN** a user runs `dml admin remote gc`
- **THEN** the command returns JSON summarizing deleted remote refs, CAS objects, and transport objects

### Requirement: Admin local GC supports dry-run inspection
`dml admin gc` SHALL garbage-collect unreachable local objects. When `--dry-run` is provided, it SHALL report what would be deleted without deleting it.

#### Scenario: Local GC deletes unreachable objects
- **WHEN** a user runs `dml admin gc`
- **THEN** the command returns JSON describing deleted local objects

#### Scenario: Local GC dry run reports orphans
- **WHEN** a user runs `dml admin gc --dry-run`
- **THEN** the command returns JSON containing `dry_run`, `would_delete`, and `orphans`
- **AND** the command does not delete local objects
