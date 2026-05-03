## ADDED Requirements

### Requirement: Local pointer refs use filesystem paths under `.dml/refs/local`
The system SHALL persist local refs as files under `.dml/refs/local/{heads,tags,indexes}`.

All `owner`, `project`, `branch`, and `tag` identifier values SHALL match `[A-Za-z0-9\-\*\|_]+`.

#### Scenario: Local branch head path
- **WHEN** a caller resolves local branch `main`
- **THEN** the pointer path is `<project_home>/.dml/refs/local/heads/main`

#### Scenario: Local tag path
- **WHEN** a caller resolves local tag `v1`
- **THEN** the pointer path is `<project_home>/.dml/refs/local/tags/v1`

#### Scenario: Local index path
- **WHEN** a caller resolves local index id `abc123`
- **THEN** the pointer path is `<project_home>/.dml/refs/local/indexes/abc123`

### Requirement: Remote-tracking refs use filesystem paths under `.dml/refs/remote`
The system SHALL persist remote-tracking refs as files under `.dml/refs/remote/<owner>/<project>/{heads,tags}`.

`dml://<owner>/<project>[#branch|@tag]` SHALL remain the user-facing parse/render shape for I/O and SHALL NOT require matching on-disk filename literals.

#### Scenario: Remote-tracking branch path
- **WHEN** a caller resolves remote branch `dml://alice/demo#main`
- **THEN** the pointer path is `<project_home>/.dml/refs/remote/alice/demo/heads/main`

#### Scenario: Remote-tracking tag path
- **WHEN** a caller resolves remote tag `dml://alice/demo@v1`
- **THEN** the pointer path is `<project_home>/.dml/refs/remote/alice/demo/tags/v1`

### Requirement: Fetch updates local remote-tracking refs only
The system SHALL keep remote S3 protocol behavior unchanged and SHALL materialize fetched tracking state into local `.dml/refs/remote/...` files.

#### Scenario: Fetch branch URI
- **WHEN** `fetch_uri("dml://alice/demo#main")` succeeds
- **THEN** local tracking file `<project_home>/.dml/refs/remote/alice/demo/heads/main` is created or updated with the fetched commit id

### Requirement: Pull into branch remains fetch then merge
The system SHALL implement pull-into-branch as fetch followed by merge.

#### Scenario: Pull branch URI
- **WHEN** `pull_uri_into_branch(uri, branch, user=...)` is invoked
- **THEN** it fetches `uri` and merges the fetched commit into local branch `branch`

### Requirement: Pointer payload format is raw commit ID
The system SHALL store only the commit ID string in each pointer file.

#### Scenario: Read pointer payload
- **WHEN** reading a pointer file for a commit
- **THEN** file content is `<commit_id>` with no `commit:` prefix

### Requirement: Pointer updates are lock-scoped and atomically replaced
The system SHALL apply lock-scoped mutation at pointer mutation sites and SHALL atomically replace pointer files for create/update operations.

#### Scenario: Concurrent pointer updates
- **WHEN** two writers race to update the same pointer
- **THEN** only one update succeeds for a given expected-current value and stale writes are rejected
