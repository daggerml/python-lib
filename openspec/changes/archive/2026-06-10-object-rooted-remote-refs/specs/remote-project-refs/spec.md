## MODIFIED Requirements

### Requirement: Project refs use typed object ref payloads
The system SHALL encode project branch and tag refs as typed remote ref payloads containing `ref.to`, `created`, and `metadata`.

Project branch and tag refs SHALL point to `commit` objects and SHALL fail before writing the ref if the target object is missing or is not a `commit` root.

Project ref `metadata` remains unconstrained in this change.

#### Scenario: Project branch ref payload
- **WHEN** project `alice/demo` branch `main` is written
- **THEN** `refs/projects/alice/demo/heads/main.json` contains `ref.to = "commit:<oid>"`, integer `created`, and object `metadata`

#### Scenario: Project tag ref payload
- **WHEN** project `alice/demo` tag `v1.0` is written
- **THEN** `refs/projects/alice/demo/tags/v1.0.json` contains `ref.to = "commit:<oid>"`, integer `created`, and object `metadata`

#### Scenario: Project ref root validation fails closed
- **WHEN** a project branch or tag ref would point to a missing object or a non-`commit` root
- **THEN** the write fails without creating or updating the project ref

### Requirement: Shared remote CAS
The system SHALL store immutable CAS objects in a shared remote CAS under `cas/sha256/<aa>/<bb>/<oid>` independent of owner, project, or branch.

#### Scenario: Two projects reference same object
- **WHEN** two project refs point to commit graphs that include the same CAS object
- **THEN** the remote stores that CAS object at one shared CAS path
