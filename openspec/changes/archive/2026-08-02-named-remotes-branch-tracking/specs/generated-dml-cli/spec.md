## ADDED Requirements

### Requirement: Generated CLI exposes named-remote synchronization commands
The generated CLI SHALL expose named remote lifecycle commands, `fetch [REMOTE]`, no-positional-argument `pull` and `push`, `branch create [--remote REMOTE] [--revision REV] NAME`, and `branch set-upstream REMOTE/BRANCH` from the public API signatures.

#### Scenario: Fetch accepts optional remote name
- **WHEN** a user runs `dml fetch research`
- **THEN** generated parsing passes `research` as the selected remote name

#### Scenario: Pull and push reject positional remotes
- **WHEN** a user runs `dml pull origin` or `dml push origin`
- **THEN** generated parsing rejects the extra positional argument

#### Scenario: Branch create options are exposed
- **WHEN** a user views `dml branch create --help`
- **THEN** help shows required positional `NAME` and optional `--remote` and `--revision` arguments
