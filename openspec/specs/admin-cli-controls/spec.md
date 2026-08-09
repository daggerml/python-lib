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

### Requirement: Admin remote list reports direct remote-root refs
`dml admin remote list` SHALL list direct branch and tag refs at resolved `remote.root`. It SHALL NOT accept project, owner, or dependency selectors and SHALL NOT perform project discovery.

#### Scenario: Remote list returns direct refs
- **WHEN** a user runs `dml admin remote list`
- **THEN** the command returns JSON containing direct `branches` and `tags` from `remote.root`

#### Scenario: Remote list rejects project selectors
- **WHEN** a user supplies a project or owner argument
- **THEN** command parsing rejects the unsupported argument

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

### Requirement: Admin exports the bundled agent skill
`dml admin agent-skill` SHALL write the complete bundled agent skill document to standard output and SHALL not write command framing or serialized representation around that document.

#### Scenario: User redirects the agent skill to a file
- **WHEN** a user runs `dml admin agent-skill > SKILL.md`
- **THEN** `SKILL.md` contains the complete bundled agent skill document
- **AND** it begins with the skill document's YAML frontmatter
