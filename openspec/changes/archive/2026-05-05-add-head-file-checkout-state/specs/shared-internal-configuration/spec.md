## MODIFIED Requirements

### Requirement: Canonical config parameters are reduced to one normalized set
The system SHALL normalize supported configuration inputs into the canonical internal parameters `project.home`, `project.uri`, `db.path`, `remote.uri`, `user`, `default_branch`, `hooks.post-init`, `hooks.post-clone`, and `config_home`.

#### Scenario: Branch context is not a canonical config parameter
- **WHEN** project configuration is resolved
- **THEN** the canonical internal model does not include a separate branch-selection parameter and does not derive the active checkout branch from configuration

#### Scenario: Legacy overlapping remote parameters are not canonical
- **WHEN** remote-backed configuration is resolved
- **THEN** the canonical remote parameter is `remote.uri` rather than separate `remote.root`, `remote.bucket`, or `remote.prefix` parameters

### Requirement: Project URI is normalized and exposes helper accessors
The system SHALL normalize and canonicalize local `project.uri` as a branchless project identity through shared revision URI utilities. Resolved configuration SHALL treat checkout state as repository state owned by `.dml/HEAD` rather than as a selector embedded in config.

#### Scenario: Local project URI remains branchless
- **WHEN** `project.uri` is resolved for local project configuration
- **THEN** shared configuration preserves canonical branchless form `dml://<owner>/<project>`

#### Scenario: Tag or branch selector is not accepted for local project config
- **WHEN** local project configuration provides `project.uri` with a branch or tag selector
- **THEN** configuration resolution fails instead of translating that selector into checkout state

#### Scenario: Project helper accessors do not expose current checkout branch
- **WHEN** resolved configuration includes `project.uri`
- **THEN** helper accessors expose project identity only and do not treat config as the source of the active branch or detached commit
