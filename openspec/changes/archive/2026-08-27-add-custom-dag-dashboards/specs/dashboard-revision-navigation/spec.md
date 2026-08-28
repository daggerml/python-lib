## MODIFIED Requirements

### Requirement: Browser routes SHALL encode canonical workspace scope
The dashboard SHALL use `/` for Home, `/projects/:project/commits/:commit` for Overview, `/projects/:project/commits/:commit/dags` and `/projects/:project/commits/:commit/dags/:dag` for DAG explorer, and `/projects/:project/commits/:commit/refs` for Tags and refs. Each path segment SHALL use percent encoding. `:project` SHALL exactly match an ID from the registered-project collection. `:commit` SHALL be a bare immutable commit ID accepted by the canonical commit-ref parser after prepending `commit:`; typed prefixes and symbolic revisions SHALL NOT appear in canonical commit routes. An initialized project without a commit SHALL use `/projects/:project/unborn` for its Overview empty state and SHALL NOT expose commit-dependent project destinations. If that route is opened after `HEAD` gains a commit, the browser SHALL replace it with the concrete Overview route. The recognized contextual query fields SHALL be `resource`, `resourceType`, `tab`, `graphFilter`, and `dashboard`, each with one string value; unknown query fields SHALL be ignored. Browser back and forward SHALL reconstruct project, commit, destination, DAG, inspector, selected custom dashboard, and known filter state from the URL without relying on local storage.

#### Scenario: Researcher opens a concrete project route
- **WHEN** a researcher navigates directly to a valid project and commit route
- **THEN** the dashboard restores that exact project, immutable commit, destination, and known contextual query state
- **AND** it does not substitute current `HEAD` or a locally stored project selection

#### Scenario: Researcher opens an unborn project route
- **WHEN** a registered readable project has no commit at `HEAD` and the researcher opens its unborn route
- **THEN** the dashboard presents only the project-scoped Overview empty state and current information that does not require a commit
- **AND** DAG explorer and Tags and refs revision selection remain unavailable until a locally inspectable commit exists

#### Scenario: Eager dashboard becomes the default selection
- **WHEN** a concrete DAG route has no `dashboard` query field and compatibility selects an eager custom dashboard by default
- **THEN** the browser replaces the route with the same scope and that dashboard's name in the `dashboard` query field

#### Scenario: Dashboard selection is incompatible
- **WHEN** a concrete DAG route names a custom dashboard that is absent or incompatible with the selected DAG
- **THEN** the browser retains the requested route and presents a bounded unavailable state without running that definition
