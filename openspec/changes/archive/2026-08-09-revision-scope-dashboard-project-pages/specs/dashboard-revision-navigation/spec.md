## Purpose

Define how the local dashboard combines cross-project awareness with project pages that inspect one explicitly selected repository commit while keeping present-day operational state temporally honest.

## ADDED Requirements

### Requirement: Home SHALL be the only global content destination
The dashboard SHALL provide one Home destination containing the cross-project status queues, rolling commit calendar, availability diagnostics, and registered-project table. The DaggerML brand link SHALL navigate to Home. Status and Projects SHALL NOT remain standalone pages or sidebar destinations, and the dashboard SHALL NOT provide compatibility routes for the removed v0 page structure.

#### Scenario: Researcher follows the brand link
- **WHEN** a researcher activates the DaggerML brand link from any dashboard page
- **THEN** the dashboard opens Home
- **AND** no project or revision is implied as the Home content scope

#### Scenario: Researcher scans cross-project state
- **WHEN** Home loads with one or more registered projects
- **THEN** it presents project selection and the existing failure-isolated live-work, availability, and commit-calendar information together
- **AND** it does not require a separate Status or Projects destination

### Requirement: Home SHALL provide a failure-isolated project directory
The Home project table SHALL list every registered project with its display name, shortened path context, last activity, current checkout summary, live-work count, sync summary, availability, and boolean `local_available` when those values can be read. Last activity SHALL be the newest known timestamp among the bounded current-`HEAD`-reachable commit projection and local live-index activity; an unavailable, absent, or truncated source SHALL be identified rather than replaced with an invented timestamp. One unreadable project SHALL NOT prevent healthy project rows or aggregate status content from rendering. A row SHALL open project context only when `local_available` is true; remote `unauthorized` or `unconfigured` state SHALL NOT disable a readable local project.

#### Scenario: Two projects have the same leaf name
- **WHEN** registered projects share a display name or leaf directory name
- **THEN** each table row exposes enough shortened parent-path context to distinguish them
- **AND** the full registered path is available on pointer hover, keyboard focus, and to assistive technology

#### Scenario: Project activity cannot be determined
- **WHEN** a registered project is unavailable or its bounded activity sources cannot establish a timestamp
- **THEN** Home presents an explicit unavailable or unknown activity value
- **AND** it does not present registration time or filesystem modification time as repository activity

#### Scenario: One project cannot be opened
- **WHEN** one registered project read fails
- **THEN** its row has `local_available = false`, does not open project context, and presents a safe project-scoped diagnostic
- **AND** other project rows and cross-project status content remain usable

### Requirement: Browser routes SHALL encode canonical workspace scope
The dashboard SHALL use `/` for Home, `/projects/:project/commits/:commit` for Overview, `/projects/:project/commits/:commit/dags` and `/projects/:project/commits/:commit/dags/:dag` for DAG explorer, and `/projects/:project/commits/:commit/refs` for Tags and refs. Each path segment SHALL use percent encoding. `:project` SHALL exactly match an ID from the registered-project collection. `:commit` SHALL be a bare immutable commit ID accepted by the canonical commit-ref parser after prepending `commit:`; typed prefixes and symbolic revisions SHALL NOT appear in canonical commit routes. An initialized project without a commit SHALL use `/projects/:project/unborn` for its Overview empty state and SHALL NOT expose commit-dependent project destinations. If that route is opened after `HEAD` gains a commit, the browser SHALL replace it with the concrete Overview route. The recognized contextual query fields SHALL be `resource`, `resourceType`, `tab`, and `graphFilter`, each with one string value; unknown query fields SHALL be ignored. Browser back and forward SHALL reconstruct project, commit, destination, DAG, inspector, and known filter state from the URL without relying on local storage.

#### Scenario: Researcher opens a concrete project route
- **WHEN** a researcher navigates directly to a valid project and commit route
- **THEN** the dashboard restores that exact project, immutable commit, destination, and known contextual query state
- **AND** it does not substitute current `HEAD` or a locally stored project selection

#### Scenario: Researcher opens an unborn project route
- **WHEN** a registered readable project has no commit at `HEAD` and the researcher opens its unborn route
- **THEN** the dashboard presents only the project-scoped Overview empty state and current information that does not require a commit
- **AND** DAG explorer and Tags and refs revision selection remain unavailable until a locally inspectable commit exists

### Requirement: Project entry SHALL resolve HEAD to a concrete commit
Selecting a project from Home, search, or the persistent project switcher SHALL open that project's Overview at the commit currently selected by `HEAD`. The dashboard SHALL resolve `HEAD` to a concrete commit identity for project-page state and SHALL expose an explicit unborn or unavailable state when no commit can be resolved. Switching projects SHALL discard the prior project's selected commit and begin the newly selected project at its own `HEAD`.

#### Scenario: Researcher selects an initialized project
- **WHEN** the project's `HEAD` resolves to a commit
- **THEN** the dashboard opens Overview with that concrete commit selected
- **AND** project identity and selected-commit identity remain visible throughout the project workspace

#### Scenario: Researcher selects an unborn project
- **WHEN** the project has no commit at `HEAD`
- **THEN** Overview presents an explicit unborn repository state
- **AND** it does not substitute a commit from another branch, tag, project, or prior selection

#### Scenario: Researcher switches projects from a historical commit
- **WHEN** the researcher uses the project switcher while viewing a non-HEAD commit
- **THEN** the newly selected project opens at its own current `HEAD`
- **AND** the previous project's commit identity is not reused

### Requirement: Project navigation SHALL preserve project and commit scope
The project sidebar SHALL contain only Overview, DAG explorer, and Tags and refs. Except for the explicit unborn Overview state, every project route and project-scoped inspector selection SHALL carry the selected project and concrete commit. Current live resources without an associated commit SHALL remain inspectable from Home or unborn Overview contextual inspection and SHALL NOT create an unscoped DAG explorer route. Changing the selected commit SHALL retain the current project page when that page supports the selected commit, clear resource selections that do not exist in the new scope, and update breadcrumbs and current-location indicators. Project navigation SHALL be hidden when no project context is active.

#### Scenario: Researcher changes commit on Overview
- **WHEN** a researcher selects another commit from the Overview commit graph
- **THEN** Overview rerenders its commit-scoped repository content for that commit
- **AND** the sidebar, breadcrumb, project switcher, and selected-commit indicator preserve orientation

#### Scenario: Selected resource is absent at the new commit
- **WHEN** a commit change makes the currently inspected DAG, node, or commit-scoped resource unavailable
- **THEN** the dashboard closes or replaces that resource selection with an explicit scoped not-found state
- **AND** it does not display detail from the previous commit as though it belongs to the new commit

### Requirement: Overview SHALL combine revision selection with project summary
Overview SHALL retain the existing checkout metrics, commit visualization, recent commits, live work, recent DAGs, and live-index timeline, except that it SHALL omit the Infrastructure card and project URI. Its commit visualization SHALL expose bounded visible repository history, identify the selected commit, and allow pointer and keyboard selection of another commit. Commit-derived summary and DAG content SHALL resolve from the selected commit rather than implicitly from the current checkout.

#### Scenario: Researcher inspects a historical commit
- **WHEN** a non-HEAD commit is selected
- **THEN** Overview presents commit-derived repository content from that commit
- **AND** the commit visualization identifies that commit while remaining available for further revision navigation

#### Scenario: Overview renders project identity
- **WHEN** Overview loads for an initialized project
- **THEN** its heading and wayfinding use the registered project identity and selected commit
- **AND** no obsolete `dml://` project URI or Infrastructure card is rendered

### Requirement: DAG explorer SHALL resolve committed graphs from the selected commit
DAG explorer SHALL list and inspect the DAG map belonging to the selected commit. It SHALL retain DAG selection, node filtering, expanded graph mode, graph legends, function-context navigation, and contextual inspection. Live or partial DAGs MAY appear only when the selected commit is the concrete commit currently resolved by `HEAD`, and they SHALL remain visibly identified as present-day live work rather than commit contents.

#### Scenario: Historical commit is selected
- **WHEN** the researcher opens DAG explorer for a commit that is not current `HEAD`
- **THEN** the inventory contains DAGs committed at that selected revision
- **AND** it omits present-day live or partial DAGs

#### Scenario: Current HEAD commit is selected
- **WHEN** DAG explorer is scoped to the concrete commit currently resolved by `HEAD`
- **THEN** it presents that commit's DAGs
- **AND** it may additionally present clearly separated live or partial DAGs with their source-index identity

### Requirement: Tags and refs SHALL expose current ref topology and select revisions
Tags and refs SHALL present current checkout state, local branch and tag tips, fetched remote-tracking branch and tag tips, bounded live main-remote branch and tag tips, configured import-only dependencies, fetched dependency refs, and bounded live dependency refs when available. It SHALL distinguish local, fetched tracking, and live remote state rather than combining them under one remote label. Selecting a branch or tag whose tip is locally inspectable SHALL resolve its tip to a concrete commit and make that commit the project workspace selection. Activating a tip that is not locally inspectable SHALL leave revision state unchanged and expose its availability explanation without fetching it.

#### Scenario: Local branch tracks a fetched and live branch
- **WHEN** the local, fetched tracking, and live remote tips are available
- **THEN** the page presents all three tip identities and the configured upstream relationship
- **AND** it reports in-sync, ahead, behind, or diverged only when the available commit graph proves that relationship

#### Scenario: Tag copies disagree
- **WHEN** local, fetched tracking, or live remote tags with the same name resolve to different commits
- **THEN** the page reports matching, local-only, remote-only, or conflicting state as applicable
- **AND** it does not describe tags as ahead or behind

#### Scenario: Dependency has fetched and live refs
- **WHEN** an import-only dependency is configured and its fetched or live refs are readable
- **THEN** the page groups those branch and tag refs beneath the dependency name and sanitized endpoint
- **AND** it distinguishes configured, fetched, live, unavailable, unauthorized, and unknown states without presenting dependency refs as local project branches

#### Scenario: Researcher selects a ref
- **WHEN** the researcher activates a branch or tag whose tip resolves to a locally inspectable commit
- **THEN** the current project page remains Tags and refs and the selected commit changes to that concrete tip
- **AND** the page marks where that selected commit sits in the current ref topology

#### Scenario: Live tip is not locally inspectable
- **WHEN** a live remote or dependency ref names a commit whose object closure is not present locally
- **THEN** the page exposes the ref and an explicit not-locally-available state
- **AND** activating it leaves the selected commit unchanged and does not fetch, materialize, or mutate repository state

### Requirement: Collection bounds SHALL be explicit
The Home aggregate SHALL use a five-minute snapshot, independent project, live-index, and recent-commit cursors, a default `limit` of 50 clamped to the inclusive range 1 through 200, a 365-day commit window, and a scan cap of 1,000 unique commits per project. It SHALL report collection continuation cursors and project-level commit truncation independently. Tags and refs SHALL inspect at most 50 configured dependencies, at most 200 branch refs, and at most 200 tag refs for each local, fetched tracking, live main-remote, or per-dependency source in one response. Its response SHALL include source-and-kind-level `truncated` booleans and safe diagnostics so omitted refs are not presented as a complete source. A client SHALL treat ref truncation as terminal for that bounded read; this change does not add cursor continuation for live ref sources.

#### Scenario: Live source exceeds its ref cap
- **WHEN** a main remote or dependency exposes more than 200 refs in one bounded source
- **THEN** Tags and refs returns no more than 200 refs for that source and marks it truncated
- **AND** other local, tracking, live, and dependency sources remain independently usable

#### Scenario: Dependency count exceeds its cap
- **WHEN** more than 50 dependencies are configured
- **THEN** Tags and refs returns at most 50 dependency groups and marks the dependency collection truncated
- **AND** it does not imply that omitted dependency state is absent

### Requirement: Historical repository state SHALL be separated from current operations
Project pages scoped to a historical commit SHALL label present-day live indexes, executions, remote availability, sync checks, and executor health as current. Current operational information SHALL NOT be attributed to the selected historical commit. Surfaces that cannot communicate both time scopes clearly SHALL omit current operational information while a historical commit is selected.

#### Scenario: Historical Overview includes live work
- **WHEN** current live work remains visible while a historical commit is selected
- **THEN** the live-work surface is explicitly labeled as current
- **AND** it does not imply that the live indexes existed at or arose from the selected commit

#### Scenario: Current remote state is unavailable
- **WHEN** a historical project page cannot read the configured remote
- **THEN** only the current operational surface reports the availability diagnostic
- **AND** commit-scoped local repository inspection remains usable

### Requirement: Revision reads SHALL remain bounded, read-only, and safe
Dashboard revision, ref, remote, and dependency reads SHALL validate project and revision identifiers, use only registered project context, preserve existing authentication and redaction guarantees, and return bounded safe diagnostics. A malformed revision identifier SHALL return terminal non-retryable `invalid-revision` with HTTP 400; the caller SHALL correct the input. A well-formed revision absent from the selected local project SHALL return terminal non-retryable `revision-not-found` with HTTP 404; the caller SHALL choose another local revision. A valid local resource outside the selected commit or allowed current live context SHALL return terminal non-retryable `resource-not-in-revision` with HTTP 404; the caller SHALL choose a matching resource or revision. A project ID absent from the registered-project collection SHALL return terminal non-retryable `project-not-registered` with HTTP 404; the caller SHALL refresh project selection. A registered project that cannot currently be opened SHALL return transient retryable `project-unavailable` with HTTP 503; the caller MAY retry and the operator action is to restore access to the registered path. Each error SHALL use the dashboard error envelope and SHALL NOT include a traceback, out-of-scope resource detail, or unregistered path. A browser route receiving one of these errors SHALL retain its requested location and render the safe error message with the corresponding retry behavior; route parsing itself SHALL NOT synthesize an HTTP response. Reading or selecting a revision SHALL NOT fetch objects, update tracking pointers, refresh refs or caches, alter checkout state, or otherwise mutate a repository or remote.

#### Scenario: Client supplies an unknown revision
- **WHEN** a project route or API request contains a revision that cannot be resolved from the selected registered project
- **THEN** the dashboard returns non-retryable `revision-not-found` with HTTP 404 and no traceback or additional internal path disclosure
- **AND** it does not fall back to `HEAD`

#### Scenario: Client supplies a malformed revision
- **WHEN** a project route or API request contains a syntactically invalid revision identifier
- **THEN** the dashboard returns non-retryable `invalid-revision` with HTTP 400
- **AND** the client does not retry without correcting the identifier

#### Scenario: Live remote inspection succeeds
- **WHEN** Tags and refs reads bounded live remote metadata
- **THEN** it returns sanitized ref and availability information
- **AND** local tracking refs and repository objects remain unchanged

### Requirement: Project APIs SHALL carry explicit revision scope
`GET /api/v1/overview`, `/api/v1/history`, `/api/v1/commits`, and `/api/v1/dags` SHALL accept required `project` and `revision` query parameters for project workspace reads. `GET /api/v1/dags/{dag_id}` and `/api/v1/nodes/{node_id}` SHALL accept the same parameters and SHALL validate that the requested resource is reachable in the selected commit or an explicitly allowed current live context. `GET /api/v1/refs` SHALL accept required `project` and `revision` plus optional boolean `live`, defaulting to true. `GET /api/v1/search` SHALL return project ID, concrete commit ID, and canonical href for project-scoped results. Unspecified query fields SHALL be ignored, and browser clients SHALL ignore unspecified additive response fields.

`/api/v1/overview` SHALL return `revision` with string `requested`, `state` equal to `ready` or `unborn`, optional string `commit`, optional string `current_head`, and boolean `is_current_head`, plus `repository` and `current` objects. `commit` SHALL be present when `state=ready` and omitted when `state=unborn`. The history and commit collections SHALL return `items` and optional `next_cursor`, with the selected commit and current tip labels available to the Overview graph. The DAG collection SHALL return `items`, revision scope, and whether current live DAGs are eligible. `/api/v1/refs` SHALL return checkout, selected revision, grouped branch refs, grouped tag refs, dependency groups, per-source diagnostics, and per-source-and-kind truncation. Fields that do not apply SHALL be omitted rather than synthesized as null.

#### Scenario: Overview resolves symbolic HEAD during project entry
- **WHEN** the client requests `/api/v1/overview` with a registered `project` and `revision=HEAD`
- **THEN** the response identifies `requested` as `HEAD` and `commit` as the concrete resolved commit ID
- **AND** subsequent workspace reads use that concrete commit rather than relying on the symbolic selector

#### Scenario: Detail does not belong to selected revision
- **WHEN** a DAG or node detail request names a valid local object that is not reachable from the selected commit or allowed current live context
- **THEN** the API returns terminal non-retryable `resource-not-in-revision` with HTTP 404
- **AND** it does not disclose the out-of-scope resource detail

### Requirement: Navigation SHALL remain usable across input modes and viewport sizes
Desktop SHALL keep project navigation and the project switcher visible in the persistent shell. Mobile SHALL provide a visible Home path, visible project-local navigation while in project context, and an accessible project-switching control. Project paths, commit graph marks, branch and tag rows, current-location states, and contextual inspector transitions SHALL support keyboard operation and accessible names in addition to pointer interaction.

#### Scenario: Keyboard user selects a commit
- **WHEN** a researcher focuses a commit in the Overview visualization and activates it from the keyboard
- **THEN** the selected revision changes exactly as it would through pointer input
- **AND** focus moves to or remains at a meaningful location in the updated project page

#### Scenario: Researcher navigates on mobile
- **WHEN** a project page is open on a narrow viewport
- **THEN** Home, the selected project, the selected commit, and all three project destinations remain discoverable
- **AND** primary project navigation is not available only through unlabeled icons or hover behavior
