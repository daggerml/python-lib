## Context

The browser currently keeps a selected project in local storage, scopes API requests with a `project` query parameter, and treats Overview and DAG reads as implicit `HEAD` reads. Global Status and Projects pages duplicate entry points, the current `/refs` projection omits import-only dependencies, and live remote refs expose names without enough tip information for useful comparison. See `proposal.md` for motivation and `specs/dashboard-revision-navigation/spec.md` for observable behavior.

The dashboard remains an internal, read-oriented presentation layer over registered local repositories. Remote reads must remain bounded and must not fetch object closures or update tracking state. Current live indexes and execution resources have no historical snapshot semantics, while commits, trees, and DAG maps do.

## Goals / Non-Goals

**Goals:**

- Give the browser one canonical project-and-commit workspace state shared by routing, API reads, breadcrumbs, navigation, search, and inspection.
- Resolve symbolic entry selectors once, then keep an immutable concrete commit selected until the researcher explicitly changes it.
- Reuse the current Overview and DAG explorer interaction models while making their repository content revision-aware.
- Project main-remote, tracking, local, and dependency refs into one bounded comparison model without mutating repository state.
- Make current operational panels unmistakable when the repository view is historical.

**Non-Goals:**

- Preserve current browser routes, page IDs, response shapes, local-storage page state, or other v0 compatibility.
- Add a project History page, perform remote fetch or synchronization, or make arbitrary live remote commits locally inspectable.
- Change commit, ref, dependency, remote, DAG, execution, or index persistence semantics.
- Add project registration management, ref mutation, checkout mutation, timezone selection, or a deployed multi-user dashboard.

## Decisions

### Canonicalize every project workspace to a concrete commit

The route hierarchy becomes:

```text
/
/projects/:project/commits/:commit
/projects/:project/commits/:commit/dags
/projects/:project/commits/:commit/dags/:dag
/projects/:project/commits/:commit/refs
/projects/:project/unborn
```

The Overview route is the commit root. Route commit segments contain the canonical bare commit ID; API calls convert it to the validated typed revision expected by core operations. Inspector resource and tab state remain query parameters on the owning page.

Home, search, and the project switcher initially request `HEAD`. The server resolves it within the selected registered project and returns the concrete commit ID. The browser immediately navigates to the concrete route. Branch and tag selection follows the same resolve-then-navigate flow. An unborn repository uses the explicit `/projects/:project/unborn` Overview state; DAG explorer and commit-dependent navigation remain unavailable until a commit exists. Current live resources without a commit remain inspectable contextually from Home or that unborn Overview and do not create an unscoped DAG route.

Repository events refresh data at the selected concrete commit. They do not silently replace it with a newer `HEAD`; the researcher can select the updated tip from the commit graph or Tags and refs. This provides stable temporal context even though no backward compatibility is required.

Alternative considered: keep `HEAD`, branch names, or tag names in project routes. Rejected because the same URL and browser state could silently represent a different repository snapshot after a ref moves.

### Use one route object as browser scope authority

Replace independent `page`, stored active-project, and implicit revision state with one parsed route object containing project ID, commit ID, destination, optional DAG ID, and inspector state. API request helpers accept project and revision explicitly rather than reading project context from local storage. The project switcher may remember only convenience state needed by Home; it is not authoritative for a project route.

Revision changes preserve the destination and clear a selected DAG, node, or inspector resource unless it is revalidated in the new commit scope. Project changes always bootstrap the new project at `HEAD` and Overview. Browser back and forward reconstruct state solely from the route.

Alternative considered: retain local storage as the API scoping source and add revision beside it. Rejected because URL, component, and request state could disagree during project switches or history navigation.

### Separate repository snapshot data from current operations in responses

Revision-aware Overview responses carry explicit scope metadata:

```text
revision.requested
revision.state
revision.commit
revision.current_head
revision.is_current_head
repository
current
```

`revision.state` is `ready` or `unborn`; commit fields are omitted for the unborn state. `repository` contains checkout-at-selection, selected commit summary, and commit-derived content. `current` contains live-index, active-execution, remote, and executor summaries. Existing endpoints may be reshaped directly because this is a v0 internal API with no compatibility requirement.

The browser renders `current` panels under visible Current labels when `is_current_head` is false. DAG explorer requests its committed inventory with the concrete revision and requests live partial DAGs only when the server confirms that the selected commit still equals current `HEAD`. If `HEAD` advances during the session, the selected commit becomes historical on refresh and live partial DAGs disappear rather than being attached to the old commit.

Alternative considered: hide all operational data at historical commits. Rejected because current live work remains useful project context on Overview, provided its time scope is explicit. Individual surfaces may still be omitted where the distinction cannot be communicated clearly.

### Keep bounded repository history on Overview as the revision selector

The history read remains a data resource but not a page destination. Overview requests a bounded ancestry projection rooted in visible local and tracking tips, identifies the current `HEAD` and selected commit, and renders the existing commit visualization plus concise commit rows. This permits movement both backward and toward another visible tip after selecting an older commit. Selecting a graph mark updates the route; commit inspection remains contextual.

The selected commit drives the Overview DAG summary and DAG explorer inventory. The graph itself describes visible repository topology and is therefore not limited to selected-commit ancestry.

Alternative considered: root the graph only at the selected commit. Rejected because viewing an older commit would remove the forward path back to `HEAD` and force an unnecessary trip through Tags and refs.

### Replace separate ref and remote projections with one comparison envelope

The project ref read becomes the authoritative Tags and refs model. It returns:

- current checkout, current `HEAD`, selected commit, and selected-commit labels;
- local branch and tag tips;
- fetched main-remote tracking branch and tag tips;
- bounded live main-remote branch and tag tips with sanitized availability diagnostics;
- configured dependency identities and sanitized roots;
- fetched and bounded live dependency branch and tag tips;
- upstream relationships, locally provable branch divergence, tag equality state, and local inspectability.

The server groups same-kind, same-name refs across sources instead of forcing the browser to correlate arrays. Ahead and behind are computed only when both tips and required ancestry are present locally. Live tips absent from local storage remain visible but unselectable and carry an unknown relation rather than triggering a fetch. Tags use equality categories, never branch divergence language.

Main-remote and dependency live reads use validated configured roots, strict per-endpoint caps, failure isolation, and safe diagnostics. They read descriptor and ref documents only. Dependency configuration is enumerated from the selected project's trusted local metadata; aggregate and detail requests never accept an arbitrary endpoint or project path from the browser.

The concrete limits are 50 configured dependencies, 200 branch refs, and 200 tag refs for each local, fetched tracking, live main-remote, or per-dependency source. Each source and kind reports truncation independently; live sources do not add cursor continuation in this change.

Alternative considered: have the browser join existing `/refs` and `/remotes` payloads. Rejected because dependency state is missing, source semantics are inconsistent, and browser-side joins cannot safely determine local object availability or ancestry.

### Enrich the existing aggregate once for Home

Home continues to use the failure-isolated `/status` snapshot as its primary data source. Project envelopes gain bounded checkout, last-activity, sync-summary, and path-context fields so the table does not issue one request per project. Last activity is calculated from the same bounded recent-commit and live-index snapshot, preserving cursor consistency and truncation metadata.

Path shortening is presentation-only. The response retains the already registered and redacted full path. A reusable path disclosure renders shortened visible text plus a focusable/hover disclosure associated through accessible description; it does not rely solely on the native `title` attribute.

Alternative considered: load `/projects` and then request every project's Overview. Rejected because it multiplies database opens and remote failure modes and can make Home internally inconsistent.

### Keep page labels and current-location cues single-sourced

The navigation model contains only Overview, DAG explorer, and Tags and refs. The same labels feed sidebar items, mobile navigation, breadcrumbs, page headings, keyboard destinations, and command-palette results. The DaggerML brand is a real Home link rather than decorative content. Home has no stale selected-project sidebar; project routes always show project name and abbreviated selected commit near the navigation and page heading.

The obsolete project URI and Infrastructure card are removed. Local availability remains a page state, remote/ref health moves to Tags and refs and current sync summaries, and execution resource health remains in current Overview panels and contextual inspectors.

Alternative considered: keep hidden Status, Projects, or History routes for convenience. Rejected because the product is v0 and hidden parallel destinations would preserve the taxonomy this change removes.

### Rebuild tests and packaged assets around the new contract

Python contract tests cover revision resolution, unknown and unborn revisions, aggregate enrichment, ref grouping, dependency failure isolation, live-tip caps, non-mutation, and redaction. React tests cover route parsing, Home entry, project switching to `HEAD`, commit selection, stale-resource clearing, historical/current labels, path disclosure, ref selection, keyboard operation, and narrow-viewport navigation. The production frontend is rebuilt into the Python package after tests pass.

Architecture documentation and the OpenSpec authority map are updated in the same implementation. The active earlier dashboard-workflow change is treated as superseded where its Status/Projects/History organization conflicts with this capability; it is not preserved in code as compatibility behavior.

## Risks / Trade-offs

- [The selected commit can become unreachable after refs move] -> Continue serving it by concrete local object ID while present; show that no current ref labels it rather than silently changing selection.
- [Home aggregate enrichment increases cross-project read cost] -> Reuse one snapshot traversal, avoid remote detail reads, cap work per project, and retain failure isolation and truncation metadata.
- [Live ref comparison can imply certainty without local ancestry] -> Emit unknown and local-inspectability flags; calculate divergence only from locally provable ancestry.
- [Historical pages mix two time scopes] -> Split response and presentation into repository snapshot and visibly current sections; omit any panel that cannot sustain that distinction.
- [A moving HEAD can remove live DAGs during refresh] -> Keep the concrete selected commit stable and explain that current live work is available only at current HEAD.
- [Removing routes and response compatibility breaks unfinished consumers] -> Accept the break explicitly for v0 and update frontend, server tests, docs, and packaged assets atomically.
- [Dependency live reads multiply remote latency and permissions failures] -> Apply independent caps and diagnostics and keep local/fetched dependency state usable when live reads fail.

## Migration Plan

1. Add revision-aware and ref-comparison read models with contract tests while retaining no frontend dependency on their old response shapes.
2. Replace the browser route/state authority and API client scoping, then build Home and the three project destinations against the new models.
3. Remove obsolete Status, Projects, History, project-URI, Infrastructure-card, old route, page-ID, and compatibility code in the same release.
4. Update architecture, security, remote-sync, and authority-map documentation.
5. Run frontend and Python quality gates, rebuild packaged static assets, and verify wheel/source-distribution inclusion.

Rollback is a source-level revert of the complete change and asset rebuild. There is no persisted-data migration and no compatibility bridge to maintain.
