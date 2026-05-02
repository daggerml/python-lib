## MODIFIED Requirements

### Requirement: Remote operations parse DML URIs
The system SHALL parse and canonicalize DML revision URIs through one centralized shared revision URI parser/stringifier boundary before deriving remote project ref paths.

#### Scenario: Push parses branch URI through shared parser
- **WHEN** push targets canonical URI `dml://alice/demo#main`
- **THEN** remote operations derive `refs/projects/alice/demo/heads/main.json` from the shared parsed revision object

#### Scenario: Fetch parses tag URI through shared parser
- **WHEN** fetch targets canonical URI `dml://alice/demo@v1.0`
- **THEN** remote operations derive `refs/projects/alice/demo/tags/v1.0.json` from the shared parsed revision object

#### Scenario: Branch/tag capability checks remain operation-specific
- **WHEN** a mutation operation targets the wrong selector type (branch op with tag URI, or tag op with branch URI)
- **THEN** the operation fails at method boundary capability checks even though URI parsing/canonicalization succeeds
