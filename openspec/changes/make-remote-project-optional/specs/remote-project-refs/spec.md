## ADDED Requirements

### Requirement: Project sync commands require configured local project URI
The system SHALL require configured local `remote.project` before resolving default project-addressed remote refs for push, pull, fetch, or checkout flows.

#### Scenario: Push without configured project URI
- **WHEN** a repository has `remote.root` but no `remote.project` and push is requested
- **THEN** push fails with a descriptive error stating that `remote.project` is required for project sync

#### Scenario: Pull without configured project URI
- **WHEN** a repository has `remote.root` but no `remote.project` and pull or fetch-by-project is requested
- **THEN** the operation fails with a descriptive error stating that `remote.project` is required for project sync

#### Scenario: Checkout on init requires configured project URI
- **WHEN** init resolves `remote.root` but not `remote.project`
- **THEN** init does not attempt project-addressed fetch or checkout
