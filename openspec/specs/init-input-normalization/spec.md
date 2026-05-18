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
