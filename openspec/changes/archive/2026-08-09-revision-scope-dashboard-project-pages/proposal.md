## Why

The dashboard currently separates global Status and Projects destinations from project pages that implicitly inspect the current checkout. Researchers need one clear cross-project home and a project workspace where every repository view is anchored to an explicitly selected commit, so they can move through history without losing page context or mistaking current operational state for historical repository state.

## What Changes

- **BREAKING** Replace the global Status and Projects destinations with a single Home destination at the DaggerML brand link. Home combines the current cross-project status content with a project-selection table.
- **BREAKING** Remove the standalone Projects page, project History page, and all legacy dashboard route compatibility. This v0 change introduces only the new route and state model.
- **BREAKING** Supersede the Status/Projects/History product organization defined by the still-active `redesign-dashboard-workflow` change; implementation reconciles that planning artifact and the dashboard authority map rather than preserving both models.
- Add a persistent project switcher. Entering or switching to a project selects `HEAD` by default.
- Scope project Overview and DAG explorer reads and routes to an explicit selected commit while retaining the selected project and page during revision changes.
- Make the Overview commit graph the history browser: selecting a commit rerenders the project workspace at that commit, without adding a separate History destination.
- Add a project-scoped Tags and refs destination that presents local, fetched tracking, live remote, and dependency state and allows branch or tag selection to choose the viewed commit.
- Distinguish immutable commit-scoped repository content from present-day operational information. Current live work, executions, remote availability, and executor health are labeled as current and are not attributed to historical commits.
- Show shortened project paths in the Home table and project switcher, reveal the full registered path on hover and keyboard focus, and preserve an accessible full value.
- Remove the obsolete project URI from project pages and remove the Overview Infrastructure card while retaining actionable health information in its authoritative current-state surfaces.

## Capabilities

### New Capabilities

- `dashboard-revision-navigation`: Cross-project Home organization, project and revision selection, revision-scoped project pages, current-state boundaries, and Tags and refs navigation.

### Modified Capabilities

None in the archived main capability suite. `dashboard-workflow-navigation` exists only in the still-active `redesign-dashboard-workflow` change, so it cannot be targeted as a main-spec MODIFIED delta; its conflicting page-organization requirements are explicitly superseded and reconciled as predecessor planning authority before implementation.

## Impact

- Dashboard frontend routing, sidebar and mobile navigation, breadcrumbs, project switcher, Home, Overview, DAG explorer, inspector context, search navigation, and API client state.
- Dashboard read models and `/api/v1` project-scoped endpoints for explicit revision selection, project activity summaries, ref comparisons, dependency refs, and bounded live remote details.
- Dashboard TypeScript models, Python response models, route and component tests, server contracts, and packaged frontend assets.
- Dashboard architecture, remote-sync architecture, security path-disclosure guidance, and OpenSpec authority mapping.
- No public Python authoring API or persisted repository object schema changes.
