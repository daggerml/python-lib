# Implementation Review

## Verdict

**Changes requested.** The implementation establishes most of the intended structure, and the existing Python and frontend suites pass, but several project-and-revision isolation paths are incorrect. The checked task ledger currently overstates completion.

## Findings

### High: Stale requests can replace data from the active project or revision

`useLoad` applies every fulfilled promise without checking whether its dependency generation is still current (`dashboard-ui/src/App.tsx:122-138`). Overview, history, DAG, ref, and execution loaders all use it across project and commit changes (`dashboard-ui/src/App.tsx:268-273`). A slow response for project A or commit A can therefore overwrite the page after navigation to project B or commit B.

Project bootstrap has the same race independently: every project selection awaits `api.overview(id, "HEAD")` and navigates when it resolves (`dashboard-ui/src/App.tsx:412-418`). Rapidly selecting B after A can still land on A if A resolves last.

This violates the canonical route-as-authority and project/revision isolation requirements. Add abort or generation guards to both loading and bootstrap, plus out-of-order response tests.

### High: Browser back and forward erase restored inspector state

`popstate` restores selection from the URL (`dashboard-ui/src/App.tsx:352-355`), but the scope-change effect then calls `closeSelection()` (`dashboard-ui/src/App.tsx:342-350`). `closeSelection()` removes the resource query fields and pushes a new history entry (`dashboard-ui/src/App.tsx:308-315`). A back navigation to a different scoped inspector route can consequently rewrite the restored URL and lose the inspector.

This contradicts `specs/dashboard-revision-navigation/spec.md:38-44`. Distinguish route restoration from user-initiated scope changes and validate the restored resource without pushing another history entry.

### High: Search results ignore their canonical project and commit route

The API contract supplies canonical `href` values for project-scoped search results, but the command palette drops `href` from its local item type and routes every non-page/project result through `onSelect` (`dashboard-ui/src/App.tsx:1063-1089`). On Home, no inspector can render because there is no scope; inside another project, the result is inspected under the currently open project and revision.

The backend also searches history from implicit `HEAD` rather than the requested revision (`src/daggerml/dashboard/read_model.py:1056-1072`). Search can therefore return content outside the selected snapshot even before the frontend misroutes it.

Preserve and navigate canonical `href`, and pass the explicit revision into the history search. Add tests with disjoint selected-revision and `HEAD` content.

### High: Nodes in reachable function-context DAGs are rejected

DAG membership validation recursively traverses nested `FnNode` context DAGs (`src/daggerml/dashboard/read_model.py:542-560`), but node membership checks only nodes directly contained by the commit's top-level DAG map (`src/daggerml/dashboard/read_model.py:562-570`). A node in a reachable function-context DAG is incorrectly returned as `resource-not-in-revision`.

Make node validation traverse the same bounded, visited DAG closure as DAG validation and add a nested function-DAG node contract test.

### High: Dependency refs do not implement the specified comparison model

Main refs are grouped by kind and name, but dependencies return unrelated `fetched` and `live` arrays (`src/daggerml/dashboard/read_model.py:421-432`; `dashboard-ui/src/types.ts:178-184`). No same-name grouping, inspectability merge, branch relation, or tag equality is computed for dependencies, despite checked tasks 3.3, 3.4, 8.1, and 8.2.

Use the same grouped ref envelope for dependency sources and cover matching, conflicting, source-only, unknown ancestry, and unavailable live dependency states.

### High: Branch divergence ignores differently named upstreams

The ref model reads a local branch's configured upstream, but computes divergence against the tracking ref already grouped under the local branch's own name (`src/daggerml/dashboard/read_model.py:392-419`). If local `main` tracks remote `trunk`, comparison uses `tracking/main` or returns unknown instead of comparing against `tracking/trunk`.

Resolve the configured upstream branch to its actual tracking tip before computing locally provable ahead/behind state. Add a differently named upstream test.

### Medium: Historical Overview labels current checkout as snapshot data

`overview()` places current `self.dml.status()` and current configuration in `repository` (`src/daggerml/dashboard/read_model.py:227-249`). The frontend normalizer derives `branch`, `head`, ahead, and behind from that current status (`dashboard-ui/src/api.ts:300-317`) and renders it as “Selected checkout” (`dashboard-ui/src/App.tsx:654-660`). On a historical commit, the selected checkout can therefore display the current branch and head.

This conflicts with `docs/develop/architecture/dashboard.md:178-185`. Move current checkout and sync data into `current`, or derive snapshot fields from the selected commit and label detached historical state accurately.

### Medium: Home live-work inspection does not open

Home rows call `openSelection()` (`dashboard-ui/src/App.tsx:535,576-583`), but the inspector is rendered only when both `selection` and concrete commit `scope` exist (`dashboard-ui/src/App.tsx:492`). Selecting a live item on Home changes the query string but shows no inspector, contrary to the explicit Home/current-resource exception in `specs/dashboard-revision-navigation/spec.md:69-70`.

Support current-resource inspection with project-only context from Home, or bootstrap a valid contextual route before opening the inspector.

### Medium: Overview does not provide the specified commit topology browser

Overview renders only the first five commits as a linear mini-list (`dashboard-ui/src/App.tsx:663-674`). It does not use the existing `CommitGraph` topology component and cannot expose bounded visible history from multiple ref tips as designed. Once the selected commit falls outside the first five, it also has no selected mark to focus.

Use the commit topology visualization or revise the contract. The current checked tasks 6.2 and 6.5 are not satisfied by a five-row list.

### Medium: Commit-scoped DAGs are placed under Current operations

The DAG inventory is loaded from the selected revision, but “Recent DAGs” is rendered inside the Current operations section (`dashboard-ui/src/App.tsx:676-700`). Historical committed DAGs are therefore mislabeled as present-day operations.

Move committed DAG summaries into Repository snapshot. Keep only live or partial DAGs in current operational content.

### Medium: Aggregate links still emit removed routes

Live-index status envelopes emit `/projects/{project}`, `/projects/{project}?resource=...`, and `/projects/{project}/dags/{dag}` (`src/daggerml/dashboard/read_model.py:737-765`). These are precisely the implicit-project routes removed by the no-compatibility contract. The frontend currently does not follow these links, but the API still publishes invalid navigation.

Emit canonical concrete-commit URLs when a current head exists, and project-only contextual links for the explicit unborn/current-resource case.

### Medium: Predecessor navigation authority remains contradictory

The predecessor spec has a supersession note, but still normatively says project selection opens Overview, History, or DAG Explorer (`openspec/changes/redesign-dashboard-workflow/specs/dashboard-workflow-navigation/spec.md:9-20`), assigns durable commit detail to History (`:92-108`), and requires mobile Status and Projects destinations (`:228-230`). `openspec/spec-overview.md:40` says responsive-evidence requirements remain authoritative, which retains the conflicting mobile labels.

Rewrite the superseded scenarios and clauses rather than relying on a broad note. Narrow retained responsive authority to inspector/evidence behavior, and annotate predecessor tasks as historical delivery superseded by this change.

### Medium: Claimed test coverage is incomplete

The checked tasks claim coverage for browser history, project-switch races, moving `HEAD`, narrow viewport behavior, dependency comparison, configured upstreams, dependency failure isolation, and nested revision resources. Current tests do not exercise popstate restoration, out-of-order requests, actual viewport behavior, differently named upstreams, grouped dependency comparison, or nested function-DAG nodes (`dashboard-ui/src/App.test.tsx:39-210`; `tests/dashboard/test_ref_envelope_contracts.py:38-100`; `tests/dashboard/test_revision_scope_contracts.py:15-370`).

Uncheck the affected tasks until behavioral tests cover these failure modes.

### Low: Packaged frontend assets are not tracked

`src/daggerml/dashboard/static/index.html:9-10` references `index-6Ma6Lykh.js` and `index-Cl8yVU1x.css`, but both files are currently untracked. A commit that omits them will package an HTML entry point whose assets do not exist.

Add the generated assets and ensure obsolete hashes are removed atomically before considering task 9.5 complete.

## Verification

- `npm test`: passed, 22 tests.
- `npm run typecheck`: passed.
- Python suite reported by the review agent: passed, 456 tests.
- Ruff reported by the review agent: passed.
- `openspec validate revision-scope-dashboard-project-pages --strict`: passed.
- `uv run --dev pyright`: failed with 13 errors in `src/daggerml/dashboard/resources.py` and `src/daggerml/dashboard/serialization.py`. Those files are outside this change's current diff, but the repository-wide type gate is not green.

## Conclusion

The broad IA and revision-routing direction is implemented, but project-and-commit scope is not yet reliable under asynchronous navigation, browser history, nested DAG inspection, search, or dependency ref comparison. Fix the high-severity findings before acceptance, then close the temporal labeling, legacy-link, authority, asset, and test-coverage gaps.
