## ADDED Requirements

### Requirement: Init identity inputs are mutually exclusive
The init operation MUST reject requests that provide both a project name and an explicit project URI, and it MUST return a descriptive validation error that explains only one identity source can be used.

#### Scenario: Name and project URI are both provided
- **WHEN** init is called with both `name` and `remote_project`
- **THEN** init fails with an error stating these inputs are mutually exclusive and one must be removed

### Requirement: Init accepts URI-only identity
The init operation MUST allow `name` to be omitted when `remote_project` is provided and MUST initialize project identity from the explicit URI.

#### Scenario: Project URI without name
- **WHEN** init is called with `remote_project` and no `name`
- **THEN** init succeeds and project configuration uses the provided project URI

### Requirement: Init derives URI from name using resolved user
When init is called with `name` and without `remote_project`, the system MUST resolve the global config user and derive the canonical project URI from that user and the provided name.

#### Scenario: Name-only init with resolved user
- **WHEN** init is called with `name`, no `remote_project`, and a resolvable global config user
- **THEN** init succeeds and stores a project URI derived from the resolved user and provided name

### Requirement: Name-based init fails when user cannot be resolved
When init is called with `name` and without `remote_project`, and global config user cannot be resolved, init MUST fail with a descriptive configuration error explaining that name-based init requires a resolved user identity.

#### Scenario: Name-only init with unresolved user
- **WHEN** init is called with `name`, no `remote_project`, and no resolvable global config user
- **THEN** init fails with an error that states user resolution is required for name-derived project URI generation
