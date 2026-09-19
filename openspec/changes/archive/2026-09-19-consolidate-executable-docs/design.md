## Context

See `proposal.md` for motivation and the three delta specs for observable documentation behavior. Start here already forms an executable dependency chain and creates a disposable `research-demo` project backed by the documentation build's isolated configuration, workspace, Moto S3 endpoint, local script executor, and Docker workflow. Use currently has twenty-five source pages, only four of which contain meaningful live workflows, and those four independently depend on `start-here/funks` rather than on one another. Extend currently has sixteen pages that repeat lifecycle, registration, packaging, and testing guidance across concept, guide, inventory, and reference documents.

`docs/build.py` validates `depends-on`, computes one topological execution order, and writes that position into each manifest page. The dashboard sorts only Start here by that position; Use and Extend retain manifest path order. QMD pages do not share interpreter memory, but they share the disposable workspace, configured object store, files, installed repository package, and persisted DaggerML projects. Jupyter is required for inline source-inspected `funkify` definitions, while knitr supports mixed Python and shell workflows. The default fixture does not own SSH, Slurm, Lambda, Batch, or arbitrary cloud credentials.

## Goals / Non-Goals

**Goals:**

- Make page order, execution prerequisites, reader progression, and dashboard order one declared graph.
- Reuse the Start here project as the durable spine of the Use course and use stable named state as an executable course contract.
- Give each extension mechanism one canonical page with build-validated implementation examples without detached plugin, packaging, or testing pages or published verification checklists.
- Preserve useful details while moving them to the audience and workflow that owns them.
- Keep the build hermetic and fail closed when fixture-owned examples or internal links regress.

**Non-Goals:**

- Change DaggerML runtime, CLI, Python, codec, adapter, executor, dashboard-provider, or storage behavior.
- Provision real remote compute services or require ambient credentials for documentation builds.
- Retain removed documentation URLs through forwarding pages or duplicate compatibility content.
- Reintroduce downloadable examples, a separate example source tree, or a second navigation-order configuration.
- Turn exhaustive API inventories or core storage internals into course material when generated help, glossary, subsystem READMEs, or OpenSpec own them better.

## Decisions

### Use one dependency graph for course execution and navigation

The three sections will be explicit chains:

```text
start-here -> get-started -> dags -> funks -> dagclasses

dagclasses -> use/projects -> use/artifacts -> use/execution
           -> use/inspection -> use/runtimes -> use/sharing

extend/codecs -> extend/adapters -> extend/executors
```

The manifest's existing `order` field remains the only ordering signal. Dashboard navigation will sort Start here, Use, and Extend pages by that field. Absolute positions may include unrelated pages; only relative order within each section matters.

Alternative considered: add a separate navigation-order field. That would permit navigation and execution to drift and create another metadata contract, so it is rejected.

### Remove Use and Extend landing pages

`docs/use/index.qmd` and `docs/extend/index.qmd` will not remain as dashboard destinations. The documentation home will describe and link to the first or relevant course pages directly. The dashboard sections themselves provide the compact inventory, so a landing page would duplicate navigation and add a click. Removed leaf pages will likewise be deleted after their useful content and inbound links are migrated.

Alternative considered: retain hidden or forwarding index pages. The current manifest exposes every rendered QMD and has no hidden-page contract; forwarding pages would either remain clutter or require a second visibility mechanism solely for compatibility.

### Continue Use from one durable research project

Projects will depend on `start-here/dagclasses`, select `research-demo`, inspect the completed tutorial state, and configure the disposable remote used by later lessons. The course will use stable names and state transitions:

```text
Projects    existing tutorial DAGs -> configured project
Artifacts   configured project     -> named input artifact and DAG
Execution   artifact DAG           -> local/Docker results and known cache entries
Inspection  completed results      -> traversed provenance and deliberate failure
Runtimes    known execution state  -> frozen/resumed runtime and refreshed cache
Sharing     completed project      -> published remote and satellite consumer projects
```

Cross-page values will be persisted as named DAGs/nodes, project configuration, files under `DOCS_WORKSPACE_ROOT`, or Moto objects. A later page will never depend on an earlier kernel variable or shell-local directory change. Destructive operations such as cache invalidation and branch experiments occur only after pages that inspect the original state; disruptive history exercises use satellite projects.

Alternative considered: make each page self-contained. That would repeat setup and fail to demonstrate the durable research lifecycle that the course is meant to teach.

### Match page engines to the workflow

Pages that define inline `funkify` callables requiring reliable source capture will use Jupyter. Pages that primarily combine CLI administration with Python inspection will use knitr. Engine changes are allowed between pages because course dependencies cross only durable boundaries. Visible fixture-owned code will assert results instead of merely printing them. Reader installation commands and unavailable infrastructure remain explicitly annotated pseudocode under the existing validator.

Alternative considered: force one engine across the course. That would either weaken inline source examples or replace ordinary CLI examples with Python subprocess wrappers that obscure the user workflow.

### Treat Use as operation of installed capabilities

The six pages divide content by researcher goal:

- Projects owns project structure, effective configuration, temporary projects, and project diagnostics.
- Artifacts owns durable values, artifact URIs, `S3Store`, external payloads, and selecting installed codecs.
- Execution owns local script and Docker composition, source isolation, cache identity, remote prerequisites, and supported execution choices.
- Inspection owns completed DAGs, named nodes, committed immutable-DAG traversal through `Projection`, Projection value/context and reuse, stored runnables and scripts, artifacts, and persisted failures.
- Runtimes owns active runtime state, freeze/resume, graph/describe, cancellation semantics, cache inspection, invalidation, recomputation, and shared effects.
- Sharing owns commits, branches, tags, diffs, remote synchronization, clone/fetch, shallow history, dependencies, and reuse.

Exact CLI, Python, configuration, state, and error details move into compact sections or tables on the page where they are applied. `dml --help` remains the exhaustive generated command inventory. Custom codec implementation moves to Codecs; dashboard-provider authoring moves to dashboard integration documentation; core execution-record storage moves to the core README; shared definitions remain in the glossary; operational hazards remain in Sharp bits and security.

Alternative considered: retain a reference hub after the six workflow pages. It would preserve the taxonomy and dashboard volume this change is intended to remove, while duplicating generated help and contextual explanations.

### Treat Adapters as the extension architecture spine

The three Extend pages will be ordered Codecs, Adapters, Executors. Adapters owns the full chronology needed to understand identity and process boundaries:

```text
funkify
  -> DelayedRunnable
  -> logical adapter lookup
  -> adapter resolves a concrete Runnable with adapter executable
     -> optionally delegates backend-specific resolution to an executor
   -> runtime launches executable from PATH or a fully specified path
  -> adapter CLI transports one operation
   -> optionally uses contrib executor dispatch for one lifecycle step
```

The page will distinguish the logical adapter key, executor key, concrete adapter executable, and target URI. Core dispatches to any CLI executable on `PATH` or at a fully specified path that implements the JSON stdin/stdout contract; it does not import adapter plugins or discover contrib executors at runtime. `AdapterBase` is an optional contrib implementation with resolution, transport, and CLI hooks. An adapter may construct a concrete `Runnable` directly; the contrib base implementation can delegate resolution to a contrib executor. The page will explain authoring-process and contrib adapter-process discovery, define invoke, cleanup, and cancel payloads, and show that repeated invoke with saved state reaches polling in the contrib executor implementation. Adapters owns this delayed-authoring, resolution, and lowering chronology. Executors then deepens the contrib-only delegated backend-specific runnable construction, lifecycle, nesting, result publication, cleanup, cancellation, remote state, and built-ins without repeating the complete wire reference. Codecs owns ordinary custom value conversion and only the narrow `ProjectionCodec` lowering mechanism.

Alternative considered: keep an Extension model or Plugin API page. Both concerns are essential, but readers apply them while implementing one of the three mechanisms; separate pages currently cause repeated and incomplete journeys.

### Execute extension examples without ambient installation

Each Extend page will directly execute its public class/factory examples and assertions. Discovery examples will inspect or exercise the repository's installed built-in entry points. Packaging metadata examples will be parsed or validated as data where practical rather than mutating the documentation Python environment. If authentic custom-package discovery needs a subprocess, the fixture will place package metadata and importable code entirely under the disposable workspace and launch that subprocess with an isolated search path; it will not install into the repository virtual environment or user site.

Published pages omit administrative metadata and implementation, test, and verification checklists. Contributor testing procedures stay in maintainer documentation; executable assertions and documentation-build validation remain required.

The course may share files or workspace-local package fixtures across pages, but it will not rely on process-global registry caches surviving between documents. Real Lambda, Batch, SSH, and scheduler behavior remains pseudocode accompanied by executable local contract examples.

Alternative considered: install a demonstration distribution into the build environment. That would make page execution order affect the maintainer's dependency environment and violate fixture isolation.

### Migrate before deleting and validate the final inventory

Implementation will maintain a content-migration matrix from every existing Use and Extend page to its destination. Canonical content moves first; inbound QMD links, `DOC_MAP.md`, Start here links, Sharp bits links, glossary links, package READMEs, and dashboard-provider references are updated before old files are removed. Validation will compare the rendered inventory to the source inventory and reject stale internal links, as it does today.

The final section inventories are:

```text
Use:    Projects, Artifacts, Execution, Inspection, Runtimes, Sharing
Extend: Codecs, Adapters, Executors
```

Alternative considered: delete duplicate pages first and reconstruct content afterward. That increases the chance of losing unique details such as projection encoding, adapter CLI behavior, two-phase plugin discovery, shallow history, script isolation, cleanup ownership, and dashboard-provider trust boundaries.

### Keep outlines useful after consolidation

Consolidated pages will use a small number of workflow-oriented section headings. Fine-grained contract details will use prose, tables, and definition lists rather than a heading for every operation because the dashboard currently includes every extracted heading in the page outline. Tests will assert the left-navigation inventory and order; visual/browser verification will ensure the active-page outline remains usable on desktop and mobile.

Alternative considered: change outline extraction as part of this work. The problem can be solved through document structure, and changing outline semantics would broaden the frontend contract unnecessarily.

## Risks / Trade-offs

- [Six comprehensive Use pages and three Extend pages become long] -> Organize each as a linear workflow with bounded top-level sections, local reference tables, and stable anchors rather than proliferating destinations.
- [Deleting routes breaks external bookmarks] -> Accept the deliberate documentation-route break, update every repository-owned link, and make the compact canonical inventory clear from the docs home and dashboard.
- [Cross-page fixture state makes failures cascade] -> Use explicit immediate dependencies, stable names, assertions at producer boundaries, and isolated satellite projects for destructive workflows.
- [The complete executable course increases build time and flakiness] -> Reuse existing built artifacts and Moto, keep one representative Docker path, avoid network/cloud dependencies, and leave exhaustive matrices in tests.
- [A cache invalidation or history exercise corrupts later assumptions] -> Schedule destructive cache work late and perform branch, shallow-history, and dependency exercises in disposable secondary projects.
- [Extension discovery examples pollute process-global caches] -> Prefer fresh page kernels and isolated subprocesses; persist only files and package metadata across pages.
- [Important reference detail is lost during consolidation] -> Require a page-by-page migration matrix and review final pages against current unique-content inventories before deletion.
- [External-infrastructure pseudocode weakens executable-doc claims] -> State the boundary honestly and pair each unavailable deployment variant with executable local protocol, lowering, or lifecycle coverage.
- [Manifest order is global rather than section-relative] -> Sort only within each dashboard section; relative topological positions remain sufficient.

## Migration Plan

1. Add tests for the target Use and Extend inventories, dependency ordering, dashboard sorting, and removal of stale internal routes.
2. Build the six Use pages in sequence against `research-demo`, migrating and asserting existing researcher workflows before removing superseded sources.
3. Build the three Extend pages in sequence, executing public examples and migrating mechanism-specific registration, packaging, built-in, and remote details; keep contributor testing procedures outside published pages.
4. Move dashboard-provider, glossary, sharp-bit, contributor, and core-architecture material to their owners and update all inbound links and `DOC_MAP.md`.
5. Remove superseded Use and Extend pages and both section landing pages; update the docs home to route readers directly into the courses.
6. Apply manifest order to all three dashboard course sections and verify keyboard, search, responsive layout, and active-page behavior.
7. Run source validation, focused documentation/frontend tests, the complete documentation build, and packaged-dashboard validation.

Rollback restores the prior page inventory, links, dashboard ordering behavior, and source dependencies together. No persisted user data or public runtime API requires migration.
