## MODIFIED Requirements

### Requirement: Project URI is normalized and exposes helper accessors
The system SHALL normalize and canonicalize local `remote.project` as an optional branchless project identity through shared revision URI utilities. Resolved configuration SHALL treat checkout state as repository state owned by `.dml/HEAD` rather than as a selector embedded in config.

#### Scenario: Local project URI remains branchless when configured
- **WHEN** `remote.project` is resolved for local project configuration
- **THEN** shared configuration preserves canonical branchless form `dml://<owner>/<project>`

#### Scenario: Local project configuration may omit project URI
- **WHEN** local project configuration omits `remote.project`
- **THEN** shared configuration resolves successfully without deriving project identity from other inputs

#### Scenario: Tag or branch selector is not accepted for local project config
- **WHEN** local project configuration provides `remote.project` with a branch or tag selector
- **THEN** configuration resolution fails instead of translating that selector into checkout state

#### Scenario: Project helper accessors do not expose current checkout branch
- **WHEN** resolved configuration includes `remote.project`
- **THEN** helper accessors expose project identity only and do not treat config as the source of the active branch or detached commit
