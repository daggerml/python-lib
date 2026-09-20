## 1. Course Contracts And Coverage

- [x] 1.1 Add documentation-source tests that assert the exact six-page Use inventory, three-page Extend inventory, immediate `depends-on` chains, project-home declarations, and absence of superseded landing/reference pages.
- [x] 1.2 Add or update manifest and dashboard tests so Start here, Use, and Extend are all expected to follow manifest execution order.
- [x] 1.3 Record a working migration checklist covering every existing `docs/use/*.qmd` and `docs/extend/*.qmd` source and its owning target before deleting any source page.

## 2. Research Project And Artifact Journey

- [x] 2.1 Consolidate project structure, effective configuration, diagnostics, and temporary-project guidance into executable `docs/use/projects.qmd`, depending on `start-here/dagclasses` and configuring the build-owned remote for later pages.
- [x] 2.2 Consolidate durable DAG values, artifact URIs, `S3Store`, external payloads, and installed-codec selection into executable `docs/use/artifacts.qmd`, consuming Projects state and producing a stable named artifact DAG for later pages.
- [x] 2.3 Add assertions for project configuration, temporary-project cleanup, artifact bytes, named nodes, and persisted provenance so both pages fail the documentation build on behavioral drift.

## 3. Research Execution And Inspection Journey

- [x] 3.1 Create executable `docs/use/execution.qmd` from the user-facing script, Docker, cache-identity, and remote-execution guidance, consuming the artifact DAG and producing stable local and wrapped execution results.
- [x] 3.2 Keep local script and Docker workflows live while labeling SSH, scheduler, Lambda, Batch, and other unowned infrastructure examples as prerequisite-specific pseudocode.
- [x] 3.3 Create executable `docs/use/inspection.qmd` that inspects the actual course DAGs, including committed immutable-DAG traversal through `Projection`, Projection values, contexts, and reuse, named nodes, artifacts, concrete and nested runnables, rendered scripts, and a deliberately persisted failure.
- [x] 3.4 Migrate contextual Python, CLI, and error-reference details into Execution and Inspection, replacing placeholder objects such as the nonexistent `analysis` DAG with assertions against course-owned state.

## 4. Research Runtime And Sharing Journey

- [x] 4.1 Consolidate runtime creation, freeze/resume, list/describe/graph behavior, cancellation semantics, cache inspection, invalidation, recomputation, cleanup, and shared effects into executable `docs/use/runtimes.qmd`.
- [x] 4.2 Make destructive runtime and cache examples consume known course executions, verify their transitions, and leave unrelated earlier results intact; use explicit contract guidance where deterministic live cancellation is not fixture-owned.
- [x] 4.3 Consolidate commits, branches, tags, diffs, remote synchronization, fetch/clone, shallow history, dependency imports, and reuse into executable `docs/use/sharing.qmd`.
- [x] 4.4 Publish the completed course project to Moto and run disruptive collaboration/history examples in stable workspace-relative satellite projects with assertions for fetched and reused results.

## 5. Extension Course

- [x] 5.1 Consolidate custom codec conversion, recursive normalization, narrow `ProjectionCodec` lowering mechanics, implementation, registration, packaging, built-ins, diagnostics, and tests into executable `docs/extend/codecs.qmd`.
- [x] 5.2 Create executable `docs/extend/adapters.qmd`, depending on Codecs, that owns the delayed authoring/lowering chronology from `funkify` through adapter and executor discovery, concrete runnable identities, executable dispatch, adapter CLI operations, installation topology, and transport tests.
- [x] 5.3 Create executable `docs/extend/executors.qmd`, depending on Adapters, that covers runnable resolution, `handle()` routing, start/poll/cleanup/cancel, durable state, idempotency, result publication, nested wrappers, remote concerns, built-ins, deployment, and lifecycle tests.
- [x] 5.4 Exercise public extension examples and installed built-in discovery without mutating the repository or user Python environment; isolate any custom distribution metadata or subprocess discovery under the documentation workspace.
- [x] 5.5 Mark real remote-transport/backend examples as pseudocode and pair them with fixture-owned lowering, payload, dispatch, or lifecycle assertions.

## 6. Content Ownership And Source Consolidation

- [x] 6.1 Move custom codec authoring out of Use, dashboard-provider contracts into dashboard integration documentation, core execution-record internals into the core README, shared definitions into the glossary, and operational hazards into Sharp bits and security.
- [x] 6.2 Update `DOC_MAP.md`, Start here, package and subsystem READMEs, contributor guidance, and all QMD cross-links to target the nine canonical Use and Extend pages or the new owning documentation.
- [x] 6.3 Verify the migration checklist against the consolidated pages, preserving Projection traversal/value/context/reuse in Inspection, narrow `ProjectionCodec` mechanics in Codecs, delayed lowering chronology in Adapters, and unique plugin-discovery, adapter CLI, script-isolation, cleanup, remote-history, and trust-boundary details.
- [x] 6.4 Delete both section landing pages and every superseded Use and Extend source only after content migration and link updates are complete.

## 7. Dashboard Navigation

- [x] 7.1 Change documentation navigation to sort Use and Extend by the existing manifest `order` field using the same behavior as Start here, without adding separate navigation metadata.
- [x] 7.2 Update frontend tests to assert exact section inventories and the declared Start here, Use, and Extend order, including reordered manifest input.
- [x] 7.3 Verify filtering, active-page state, keyboard navigation, heading outlines, and desktop/mobile layout with the smaller course inventories and consolidated page headings.

## 8. Validation

- [x] 8.1 Run OpenSpec strict validation and documentation source validation; resolve malformed dependencies, non-executable unmarked examples, stale routes, and missing anchors.
- [x] 8.2 Run focused documentation-build, dashboard frontend, server, and distribution contract tests required by the changed paths.
- [x] 8.3 Run the complete `docs/build.sh` workflow and confirm it executes all course examples, packages exactly the intended pages, and leaves ambient DaggerML and cloud state untouched.
- [x] 8.4 Review the rendered dashboard in dark and light themes at desktop and mobile sizes, confirming course order, readable outlines, no horizontal overflow, and no browser errors.

## 9. Follow-up Ownership Audit

- [x] 9.1 Audit the completed documentation reorganization against reader-goal ownership while preserving the exact six Use and three Extend pages and their order.
- [x] 9.2 Correct the ownership records in place: Projection inspection in Inspection, durable values and installed-codec selection in Artifacts, `ProjectionCodec` mechanics in Codecs, delayed lowering in Adapters, and runnable lifecycle in Executors.
- [x] 9.3 Validate the corrected ownership records with source validation, the focused documentation ownership test, and strict OpenSpec validation.
