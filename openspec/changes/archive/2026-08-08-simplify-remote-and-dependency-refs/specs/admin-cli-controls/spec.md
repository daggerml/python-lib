## ADDED Requirements

### Requirement: Admin remote list reports direct remote-root refs
`dml admin remote list` SHALL list direct branch and tag refs at resolved `remote.root`. It SHALL NOT accept project, owner, or dependency selectors and SHALL NOT perform project discovery.

#### Scenario: Remote list returns direct refs
- **WHEN** a user runs `dml admin remote list`
- **THEN** the command returns JSON containing direct `branches` and `tags` from `remote.root`

#### Scenario: Remote list rejects project selectors
- **WHEN** a user supplies a project or owner argument
- **THEN** command parsing rejects the unsupported argument

## REMOVED Requirements

### Requirement: Admin remote list can list projects or one project's refs
**Reason**: One `remote.root` represents one project and no project discovery or project URI exists.
**Migration**: Run `dml admin remote list` without a project selector to list direct refs.
