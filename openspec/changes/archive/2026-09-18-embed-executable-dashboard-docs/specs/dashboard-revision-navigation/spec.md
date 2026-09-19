## RENAMED Requirements

- FROM: `Home SHALL be the only global content destination`
- TO: `Home and Docs SHALL be global content destinations`

## MODIFIED Requirements

### Requirement: Home and Docs SHALL be global content destinations
The dashboard SHALL provide one Home destination containing the cross-project status queues, rolling commit calendar, availability diagnostics, and registered-project table, plus a Docs destination containing packaged documentation and examples. Both SHALL be global and independent of project/revision scope. The DaggerML brand link SHALL navigate to Home. Status and Projects SHALL NOT remain standalone pages or sidebar destinations, and the dashboard SHALL NOT provide compatibility routes for the removed v0 page structure.

#### Scenario: Researcher follows the brand link
- **WHEN** a researcher activates the DaggerML brand link from any dashboard page
- **THEN** the dashboard opens Home
- **AND** no project or revision is implied as the Home content scope

#### Scenario: Researcher scans cross-project state
- **WHEN** Home loads with one or more registered projects
- **THEN** it presents project selection and the existing failure-isolated live-work, availability, and commit-calendar information together
- **AND** it does not require a separate Status or Projects destination

#### Scenario: Researcher opens Docs from a historical workspace
- **WHEN** a researcher selects Docs while inspecting a historical commit
- **THEN** the dashboard opens global documentation inside the persistent shell without treating it as content of that commit
- **AND** browser back can restore the prior project route and its encoded revision context
