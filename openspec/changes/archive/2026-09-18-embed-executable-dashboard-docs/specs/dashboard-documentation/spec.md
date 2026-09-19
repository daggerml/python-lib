## Purpose

Provide version-matched documentation and downloadable examples as global content inside the existing dashboard workbench.

## ADDED Requirements

### Requirement: Docs SHALL render inside the persistent dashboard layout
The dashboard SHALL expose Docs as global content using its existing persistent shell, responsive navigation, keyboard access, and light/dark themes. Docs SHALL not require a selected project, a commit, or configured remote services, and SHALL not be an iframe or separate website shell.

#### Scenario: No project is configured
- **WHEN** a reader opens Docs without a registered project
- **THEN** packaged documentation remains readable inside the dashboard layout

#### Scenario: Reader uses mobile or keyboard navigation
- **WHEN** a reader navigates documentation on a narrow viewport or with a keyboard
- **THEN** Home, Docs hierarchy, current page, and download links remain discoverable and operable

### Requirement: Documentation routes SHALL be addressable and distinct from API documentation
The dashboard SHALL use `/docs` for documentation home and `/docs/<page>` for nested pages, including `/docs/examples/<example>`. API Swagger documentation SHALL move to `/api/docs`. Direct navigation, refresh, anchors, and browser history SHALL preserve the requested documentation location without inferring project/revision scope.

#### Scenario: Reader opens a deep link
- **WHEN** a reader opens or refreshes a nested documentation URL with a heading fragment
- **THEN** the dashboard restores the page and heading inside its persistent shell

#### Scenario: Reader opens API documentation
- **WHEN** a reader requests `/api/docs`
- **THEN** the server serves Swagger UI without colliding with dashboard Docs

### Requirement: Documentation assets and downloads SHALL be served safely
Packaged pages SHALL resolve internal links, assets, and downloads without external build tooling. Unknown docs pages SHALL show an explicit not-found state, and missing content assets/downloads SHALL return not-found rather than SPA HTML. Static serving SHALL preserve path containment and existing API security guarantees. Published output SHALL not disclose fixture credentials or private execution state.

#### Scenario: Download is missing
- **WHEN** a reader requests an unknown download path
- **THEN** the server returns a not-found response rather than the dashboard index

#### Scenario: Reader follows an example download
- **WHEN** a valid download link is activated
- **THEN** the browser receives the intended script or bundle from packaged static assets
