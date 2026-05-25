### Requirement: Top-level CLI uses git-shaped repository inspection verbs
The public `dml` CLI SHALL expose repository-oriented porcelain commands at the top level: `status`, `show`, `log`, `diff`, `checkout`, `branch`, `fetch`, `pull`, `push`, `merge`, and `revert`.

#### Scenario: Top-level help reflects git-shaped porcelain
- **WHEN** a user inspects the top-level CLI surface
- **THEN** the documented primary commands are `status`, `show`, `log`, `diff`, `checkout`, `branch`, `fetch`, `pull`, `push`, `merge`, and `revert`

### Requirement: Status reports repository state instead of config state
`dml status` SHALL report current repository/runtime status as JSON, including the current HEAD state, available local branches, DAG map for the current revision, and live indexes.

#### Scenario: Status returns repository summary
- **WHEN** a user runs `dml status`
- **THEN** the command returns JSON with `head`, `branches`, `dags`, and `indexes` fields

### Requirement: Show returns commit metadata, full DAG map, and commit delta
`dml show <revision>` SHALL resolve the revision locally and return JSON with top-level `revision`, `commit`, `dags`, and `change` fields.

The `dags` field SHALL be the full DAG name-to-ref map for the resolved revision. The `change` field SHALL describe the DAG-map delta introduced by the resolved commit relative to its base commit.

#### Scenario: Show returns full DAG map and change
- **WHEN** a user runs `dml show HEAD`
- **THEN** the command returns JSON containing `revision`, `commit`, `dags`, and `change`
- **AND** `dags` contains the complete DAG map for the resolved commit

#### Scenario: Show root commit uses empty base
- **WHEN** a user runs `dml show` on a root commit with no parents
- **THEN** `change.base` is `null`
- **AND** every DAG in `dags` appears as an addition in `change`

#### Scenario: Show merge commit uses first parent as base
- **WHEN** a user runs `dml show` on a merge commit with multiple parents
- **THEN** `change` is computed relative to the first parent commit

### Requirement: Diff compares DAG maps between revisions
`dml diff [<left>] [<right>]` SHALL compare two locally resolved revisions and return DAG-map differences as JSON `added`, `removed`, and `updated` sections.

#### Scenario: Diff returns DAG map changes
- **WHEN** a user runs `dml diff main feature`
- **THEN** the command returns JSON with `left`, `right`, `added`, `removed`, and `updated` fields

### Requirement: Log returns commit entries for a revision walk
`dml log [<revision>] [--limit N]` SHALL return commit entries starting from the resolved revision, defaulting to `HEAD`.

#### Scenario: Log defaults to HEAD
- **WHEN** a user runs `dml log`
- **THEN** the command resolves `HEAD`
- **AND** returns JSON containing `revision` and `commits`

### Requirement: Branch listing and creation support local and remote-tracking workflows
`dml branch` SHALL list local branches. `dml branch <name>` SHALL create a local branch from the current HEAD commit. `dml branch -r` and `dml branch --remote` SHALL list remote-tracking branches.

#### Scenario: Branch lists local branches by default
- **WHEN** a user runs `dml branch`
- **THEN** the command returns JSON with a `branches` field containing local branch names

#### Scenario: Branch lists remote-tracking branches
- **WHEN** a user runs `dml branch --remote`
- **THEN** the command returns JSON with a `branches` field containing remote-tracking branch selectors

#### Scenario: Branch creates a local branch from the current head
- **WHEN** a user runs `dml branch feature`
- **THEN** the command creates local branch `feature` from the current HEAD commit
- **AND** returns the created branch name

### Requirement: DAG inspection is organized under `dml dag`
The CLI SHALL expose DAG-oriented inspection commands under `dml dag`: `list`, `get`, `checkout`, and `delete`.

#### Scenario: DAG commands are grouped under dag
- **WHEN** a user inspects DAG-related CLI help
- **THEN** DAG inspection and DAG tree mutation commands appear under `dml dag`

### Requirement: DAG list returns revision-scoped DAG map
`dml dag list [--revision REV]` SHALL return the DAG name-to-ref map for the selected revision as JSON.

#### Scenario: DAG list returns mapping
- **WHEN** a user runs `dml dag list --revision HEAD~1`
- **THEN** the command returns JSON with `revision` and `dags`
- **AND** `dags` is an object mapping DAG names to DAG refs

### Requirement: DAG get resolves by name or exact DAG ref
`dml dag get <name-or-id> [--revision REV]` SHALL resolve either a DAG name within a revision's DAG map or an explicit `dag:<id>` selector.

If the selector is `dag:<id>`, the command SHALL reject any provided `--revision` flag.

#### Scenario: DAG get resolves name in revision
- **WHEN** a user runs `dml dag get train --revision HEAD~1`
- **THEN** the command resolves `train` in the DAG map for `HEAD~1`
- **AND** returns JSON containing `selector`, `revision`, and `dag`

#### Scenario: DAG get loads exact DAG ref
- **WHEN** a user runs `dml dag get dag:abc123`
- **THEN** the command loads that exact DAG object
- **AND** returns JSON containing `selector` and `dag`

#### Scenario: DAG get rejects revision with explicit DAG ref
- **WHEN** a user runs `dml dag get dag:abc123 --revision HEAD`
- **THEN** the command fails without resolving a revision

### Requirement: DAG get includes node data
The `dml dag get` payload SHALL include the DAG's node data so that users do not need a separate DAG-node inspection endpoint for normal CLI workflows.

#### Scenario: DAG get includes nodes
- **WHEN** a user runs `dml dag get train`
- **THEN** the returned `dag` object includes node-level data needed for DAG inspection
