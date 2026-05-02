## Purpose
Establish a single contract-matrix owner for revision/ref/URI parsing behaviors so grammar coverage is centralized and workflow tests stay focused on operational invariants.

## Requirements

### Requirement: Revision and URI parsing contracts are centrally owned by one parameterized matrix suite
The repository SHALL define revision/ref/URI parsing behavior in one maintained contract test suite that uses parameterized case matrices rather than duplicating equivalent parsing assertions across workflow tests.

#### Scenario: Parsing contract matrix is the single maintained owner
- **WHEN** maintained tests assert behavior for `parse_ref`, DML URI canonicalization, or revision-form resolution
- **THEN** those assertions are implemented in the centralized parsing contract matrix suite instead of being repeated across unrelated workflow contract files

#### Scenario: Workflow contracts avoid duplicate parsing assertions
- **WHEN** a workflow contract test validates delegation, state transitions, or side-effect invariants
- **THEN** it uses canonical valid inputs and does not re-assert grammar-level parsing variants already covered by the parsing matrix

### Requirement: Parsing matrix cases include canonical contract IDs and explicit case labels
The centralized parsing matrix SHALL encode each case with direct canonical contract IDs and readable case labels in parameterized IDs.

#### Scenario: Parameterized parsing case includes direct canonical ID
- **WHEN** a parsing behavior case is defined via parameterization
- **THEN** the case `id=` includes a direct literal canonical contract ID and a human-readable case label

#### Scenario: Parsing case failures remain traceable
- **WHEN** a parsing matrix case fails
- **THEN** the failing node identifier includes both the contract ID and case label needed to identify the exact parsing form boundary

### Requirement: Revision-form matrix covers accepted and rejected local resolution boundaries
The centralized parsing matrix SHALL cover the accepted revision forms and local-only rejection boundaries required by commit/project revision resolution behavior.

#### Scenario: Accepted revision forms resolve with expected classification
- **WHEN** the suite evaluates accepted revision forms (branch, tag, ancestry expression, direct commit id, explicit commit ref)
- **THEN** each form resolves to the expected classification and commit target for the fixture setup

#### Scenario: Unfetched remote revision form fails with local-resolution boundary
- **WHEN** a `dml://...#<branch>` revision form is evaluated without corresponding local tracking state
- **THEN** resolution fails with the documented local-resolution boundary error indicating fetch is required
