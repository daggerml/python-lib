## MODIFIED Requirements

### Requirement: Revision-form matrix covers accepted and rejected local resolution boundaries
The centralized parsing matrix SHALL cover the accepted revision forms and local-only rejection boundaries required by commit/project revision resolution behavior, including file-backed `HEAD` semantics.

#### Scenario: Accepted revision forms resolve with expected classification
- **WHEN** the suite evaluates accepted revision forms (branch, tag, ancestry expression, direct commit id, explicit commit ref, and `HEAD` backed by `.dml/HEAD`)
- **THEN** each form resolves to the expected classification and commit target for the fixture setup

#### Scenario: Detached HEAD ancestry resolves from HEAD file
- **WHEN** `.dml/HEAD` contains a detached commit payload and the suite evaluates `HEAD~1`
- **THEN** resolution walks ancestry from the detached commit stored in `.dml/HEAD`

#### Scenario: Unfetched remote revision form fails with local-resolution boundary
- **WHEN** a `dml://...#<branch>` revision form is evaluated without corresponding local tracking state
- **THEN** resolution fails with the documented local-resolution boundary error indicating fetch is required
