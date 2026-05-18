## REMOVED Requirements

### Requirement: Init identity inputs are mutually exclusive
**Reason**: Init no longer accepts `name` as an alternate identity source.
**Migration**: Pass `remote_project` explicitly when project publication identity is needed.

### Requirement: Init accepts URI-only identity
**Reason**: Init now accepts `remote_project` as an optional capability input rather than as the sole way to omit `name`.
**Migration**: Continue passing `remote_project` when desired, but do not pass `name`.

### Requirement: Init derives URI from name using resolved user
**Reason**: Name-derived project identity is removed.
**Migration**: Provide `remote_project` explicitly instead of relying on user-derived URI generation.

### Requirement: Name-based init fails when user cannot be resolved
**Reason**: Init no longer derives project identity from user configuration.
**Migration**: Omit project identity for local-only init or pass explicit `remote_project`.

## ADDED Requirements

### Requirement: Init accepts optional remote capabilities
The init operation MUST accept optional `remote_project` and optional `remote_root` inputs. Init MUST allow both values to be omitted for local read-only repository bootstrap.

#### Scenario: Init without remote configuration
- **WHEN** init is called with no `remote_project` and no `remote_root`
- **THEN** init succeeds without deriving or persisting project publication identity

#### Scenario: Init with remote root only
- **WHEN** init is called with `remote_root` and no `remote_project`
- **THEN** init succeeds and configures remote-backed mutation and execution capability without project sync capability

### Requirement: Init rejects project identity without remote root
The init operation MUST reject `remote_project` when `remote_root` is absent.

#### Scenario: Project URI without remote root
- **WHEN** init is called with `remote_project` and no `remote_root`
- **THEN** init fails with a descriptive validation error stating that `remote.root` is required when `remote.project` is configured
