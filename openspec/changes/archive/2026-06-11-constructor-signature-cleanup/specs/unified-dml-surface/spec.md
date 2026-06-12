## MODIFIED Requirements

### Requirement: Shared `Dml` constructor uses root runtime override inputs
The shared `Dml` constructor SHALL accept the full supported runtime configuration surface through Python-friendly keyword parameters, including project, database, remote, default, and user/config-home overrides. `Dml.init(...)` SHALL accept the same configuration kwargs plus bootstrap-only parameters. The shared surface SHALL also expose a classmethod for constructing `Dml` from flattened canonical config-var dictionaries.

#### Scenario: Python kwargs cover the supported config surface
- **WHEN** a caller provides explicit configuration overrides supported by the shared resolver
- **THEN** those values can be passed directly to the shared `Dml` constructor using Python-friendly parameter names

#### Scenario: Init reuses constructor config kwargs
- **WHEN** a caller provides supported configuration overrides to `Dml.init(...)`
- **THEN** the init workflow accepts the same config kwargs as `Dml.__init__` in addition to bootstrap-only args

#### Scenario: Canonical config vars use dedicated classmethod
- **WHEN** a caller already has a flattened config-var dictionary such as `{"remote.root": "s3://bucket/root"}`
- **THEN** it can construct a `Dml` instance through the dedicated config-var classmethod without translating those keys to Python kwargs first
