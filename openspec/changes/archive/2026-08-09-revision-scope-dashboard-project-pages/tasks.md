## 1. Revision-Aware Server Contracts

- [x] 1.1 Reconcile the active `redesign-dashboard-workflow` proposal, delta spec, design, and authority-map entry so its Status, Projects, and History organization is explicitly superseded by this capability before implementation begins.
- [x] 1.2 Add dashboard response models for requested revision, resolved commit, current `HEAD`, `is_current_head`, commit-scoped repository data, and explicitly current operational data.
- [x] 1.3 Implement registered-project revision resolution for `HEAD` and concrete commits, including stable unborn, malformed, unknown, and cross-project failure responses without fallback.
- [x] 1.4 Make Overview, commit summary, history projection, DAG collection, DAG detail, node detail, and relevant search links accept and return explicit project-and-revision scope.
- [x] 1.5 Add Python contract tests proving concrete revisions remain stable when `HEAD` moves and that revision reads remain bounded, read-only, redacted, and project-isolated.
- [x] 1.6 Add server failure-matrix tests for `invalid-revision` 400, `revision-not-found` 404, `resource-not-in-revision` 404, `project-not-registered` 404, and retryable `project-unavailable` 503, including safe diagnostics and no `HEAD` fallback.

## 2. Home Aggregate Model

- [x] 2.1 Enrich failure-isolated Status project envelopes with checkout summary, current head, bounded last activity, live-work count, sync summary, path context, availability, and truncation or unknown state.
- [x] 2.2 Calculate last activity from the same snapshot's reachable commits and live indexes without substituting registration or filesystem timestamps.
- [x] 2.3 Add aggregate contract tests for healthy, unavailable, unborn, duplicate-name, absent-activity, and truncated projects while preserving independent cursor behavior.

## 3. Tags And Refs Read Model

- [x] 3.1 Project local branches, local tags, fetched main-remote tracking refs, upstreams, current checkout, current `HEAD`, and selected-commit labels into one grouped ref envelope.
- [x] 3.2 Add bounded live main-remote branch and tag tip reads with local-inspectability and safe per-source availability diagnostics, without fetching or updating tracking refs.
- [x] 3.3 Enumerate configured import-only dependencies and project their sanitized roots, fetched refs, bounded live refs, and independent availability diagnostics.
- [x] 3.4 Compute branch in-sync, ahead, behind, diverged, or unknown state only from locally provable ancestry, and compute tag matching, source-only, or conflicting state by tip equality.
- [x] 3.5 Add server and read-model contract tests for ref grouping, local-only and remote-only tips, conflicts, unknown ancestry, unmaterialized live commits, dependency failure isolation, caps, redaction, and repository non-mutation.

## 4. Canonical Browser Scope And Routing

- [x] 4.1 Replace independent page, local-storage project, and implicit revision state with a parsed route model for Home and concrete project/commit Overview, DAG explorer, Tags and refs, DAG, and inspector locations.
- [x] 4.2 Refactor API helpers and event subscriptions to receive project and revision scope explicitly rather than reading active project state from local storage.
- [x] 4.3 Implement project bootstrap that resolves `HEAD` and navigates to the concrete commit route, including unborn and unavailable project states.
- [x] 4.4 Preserve the current project destination across commit changes, clear or revalidate stale resource selections, and make browser back/forward reconstruct all scope from the URL.
- [x] 4.5 Remove old Status, Projects, History, implicit project, and compatibility route handling instead of retaining redirects or adapters.
- [x] 4.6 Add frontend tests for route parsing, direct entry, `HEAD` bootstrap, project switching, moving `HEAD`, commit changes, stale inspector state, browser history, and safe error rendering that retains the requested URL.
- [x] 4.7 Add negative route tests proving standalone Status, Projects, History, and every prior implicit-project route are unreachable and do not redirect or load compatibility UI.

## 5. Home And Project Navigation

- [x] 5.1 Replace standalone Status and Projects pages with one Home that renders the project table, existing status queues, commit calendar, and project diagnostics from the aggregate snapshot.
- [x] 5.2 Build an accessible project path disclosure with shortened visible context and full path on pointer hover, keyboard focus, and assistive description.
- [x] 5.3 Make the DaggerML brand a Home link and reduce project navigation to Overview, DAG explorer, and Tags and refs with shared labels, active states, breadcrumbs, and selected-commit context.
- [x] 5.4 Keep a persistent accessible project switcher in project context and make every project selection start at that project's resolved `HEAD` Overview.
- [x] 5.5 Update desktop and mobile navigation so Home and all project destinations remain visible and understandable without hover-only or unlabeled-icon interaction.
- [x] 5.6 Add component tests for Home failure isolation, project-table fields, duplicate-name path context, path disclosure input modes, brand navigation, switcher behavior, current-location cues, and narrow viewports.

## 6. Revision-Scoped Overview

- [x] 6.1 Reshape Overview rendering around explicit repository snapshot and current-operation sections, with visible Current labels whenever a historical commit is selected.
- [x] 6.2 Make the bounded commit visualization identify the selected commit and support pointer and keyboard revision selection while retaining visible history from repository ref tips.
- [x] 6.3 Resolve commit-derived metrics, recent commits, and recent DAG summaries from the selected commit while keeping current live work and live-index timing temporally distinct.
- [x] 6.4 Remove the project URI and Infrastructure card, retaining local failure states, ref and remote health links, execution health, and resource evidence in their authoritative surfaces.
- [x] 6.5 Add Overview tests for historical commits, advancing `HEAD`, current-operation labels, commit selection and focus, selected-commit highlighting, and removed content.

## 7. Revision-Scoped DAG Explorer

- [x] 7.1 Load the DAG inventory and DAG details from the selected concrete commit while preserving selector, graph filter, expanded mode, legends, context DAG navigation, and inspector integration.
- [x] 7.2 Include clearly separated live or partial DAGs only when the selected commit still equals current `HEAD`, and remove them on refresh when that relationship changes.
- [x] 7.3 Prevent DAG and node details from a previous revision from surviving a commit change unless they validate in the new scope.
- [x] 7.4 Add frontend and server tests for historical inventories, current-HEAD partial DAGs, moving-HEAD refresh, route-addressed DAGs, and revision-scoped not-found behavior.

## 8. Tags And Refs Page

- [x] 8.1 Build checkout and selected-commit summaries plus grouped branch, tag, main-remote, and dependency sections from the unified ref envelope.
- [x] 8.2 Present source-specific tips, upstreams, availability, inspectability, branch divergence, tag equality, and unknown states without conflating fetched tracking and live remote data.
- [x] 8.3 Make locally inspectable branch and tag tips keyboard- and pointer-selectable revision controls that retain the Tags and refs destination and mark the selected commit.
- [x] 8.4 Keep unmaterialized live tips visible but non-mutating and explain why they cannot be selected without offering an implicit fetch.
- [x] 8.5 Add component tests for every comparison state, dependency grouping, safe diagnostics, selected-ref navigation, unavailable tips, keyboard behavior, and mobile layout.

## 9. Documentation And Verification

- [x] 9.1 Update `docs/develop/architecture/dashboard.md`, `docs/develop/architecture/system-overview.md`, `docs/develop/architecture/remotes-and-sync.md`, `docs/sharp-bits-and-security.md`, and `openspec/spec-overview.md` for the new authority, routes, response boundaries, path disclosure, and no-compatibility v0 organization.
- [x] 9.2 Update affected dashboard and API tests to remove assumptions about standalone Status, Projects, History, project URI, Infrastructure, local-storage request scope, and old route or response compatibility.
- [x] 9.3 Run dashboard frontend tests and type checking with `npm test` and `npm run typecheck` in `dashboard-ui/`, then run the production frontend build.
- [x] 9.4 Run Python lint fixing and fast tests with `uv run --dev --all-extras ruff check --fix .` and `uv run --dev --all-extras pytest -m "not slow" .`, then run the full test suite.
- [x] 9.5 Rebuild packaged dashboard assets and verify wheel and source-distribution contents serve the new application without a runtime Node.js dependency.
- [x] 9.6 Run strict OpenSpec validation and verify the reconciled authority mapping contains no unresolved contradictory navigation requirements.
