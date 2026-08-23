## MODIFIED Requirements

### Requirement: Shallow history SHALL be explicit local state
The system SHALL record each intentionally unavailable commit ref in repository-local `.dml/shallow.json` using the exact object `{"version": 0, "missing": [...]}`. The version SHALL be a non-boolean integer equal to `0`, and `missing` SHALL be a sorted, unique array containing only exact commit refs. The reader SHALL reject every other version, unknown or missing field, malformed ref, wrong type, duplicate, or unsorted value. The metadata SHALL NOT alter immutable commit, tree, DAG, remote ref, or remote CAS representations. An unavailable commit ref not recorded by this metadata SHALL be treated as repository corruption rather than a valid shallow boundary.

#### Scenario: Omitted parent is recorded
- **WHEN** depth-limited materialization stops before an unavailable commit parent
- **THEN** that exact parent commit ref is recorded in version-0 shallow metadata before the fetched tracking ref becomes visible

#### Scenario: Initial shallow schema is exact
- **WHEN** shallow metadata is written
- **THEN** it contains exactly `version` with non-boolean integer value `0` and `missing` with sorted unique commit refs

#### Scenario: Unsupported shallow shape fails closed
- **WHEN** shallow metadata contains another version, an extra or missing field, or malformed `missing` content
- **THEN** repository access fails with an invalid-shallow-metadata error instead of accepting or migrating it

#### Scenario: Missing non-history object is corruption
- **WHEN** a materialized commit's tree or DAG closure refers to an unavailable object
- **THEN** the operation fails as an incomplete or corrupt repository even if shallow-history metadata exists

#### Scenario: Immutable identity is preserved
- **WHEN** a shallow commit is materialized
- **THEN** its parent list and content-derived commit identity remain identical to the remote object
