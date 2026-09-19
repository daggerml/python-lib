## Context

See proposal.md for motivation. Docs are relative-linked Markdown; examples have shell-based orchestration and isolated Moto-backed execution. The React dashboard uses handwritten routes, while FastAPI currently owns `/docs` for Swagger. Vite clears `dashboard/static/` on build. Release wheel and sdist jobs independently build frontend assets. There is no Quarto integration.

## Goals / Non-Goals

**Goals:** one executable documentation source, native language execution, deterministic failure reporting, static delivery inside the existing workbench, and no package dependency changes.

**Non-Goals:** browser-side code execution, a separate Quarto website shell, iframe embedding, cross-kernel environment propagation, migrating every shell workflow, or changing core execution semantics.

## Decisions

### Use knitr throughout

Set `engine: knitr` for the docs project. `{python}` cells execute through reticulate using the project interpreter selected by `RETICULATE_PYTHON`; `{bash}` cells execute through knitr's Bash engine. Do not use Python subprocess APIs, shell magics, or another Python library to execute shell commands. R, Quarto, knitr, rmarkdown, and reticulate belong only to the build environment. Pin/document supported build-tool versions separately from package runtime and optional dependencies.

Jupyter was rejected because mixed-language pages would require a second execution arrangement or Python shell wrappers. A first implementation gate must prove reticulate can execute representative `funkify(uri="script")` and dagclass examples, including source inspection. Resolve any source-filename issue in build-only cell handling, not core APIs; an incompatible engine requires revisiting the design rather than silent exemptions.

### Make execution fail closed

Use `eval: true`, `error: false`, `cache: false`, and `freeze: false`. Hide plumbing with `include: false`, and suppress only results with `output: false`. Enforce these invariants across project, page, and cell overrides, including native knitr evaluation/error options. Lint authored code fences: executable cells or explicitly identified pseudocode only. Literal data/config examples must be constructed and validated by executable cells, not exempted as another language; diagrams and generated output are not authored runnable code.

Configure Bash cells with strict shell flags equivalent to `set -euo pipefail` without showing boilerplate. Verify knitr's actual nonzero-status handling and promote shell failures to rendering errors if necessary through build-only R engine configuration. Assert expected exceptions/statuses narrowly instead of enabling permissive error rendering. Test Python exceptions, Bash failures, failed pipelines, setup failure, render failure, and cleanup failure against the top-level build exit status. Emit page/cell diagnostics. Cleanup must preserve the original failure and also fail an otherwise successful build if cleanup fails.

### Keep infrastructure outside the lesson

Provision disposable services and per-page DML/config/scratch roots. A hidden Bash bootstrap QMD owns service setup and writes the environment needed by the renderer; the outer Bash build coordinator imports that environment before launching page renders and guarantees teardown through an exit trap. Hidden Bash teardown QMD owns cleanup commands. This is parent-to-child environment inheritance, not cell/kernel propagation. Persist cleanup ownership as resources are created so partial setup is recoverable.

Moto provides S3/CloudWatch; SSH and other infrastructure are provisioned only when included examples need them. A selected example's missing prerequisite fails the build rather than skipping it. Never use the developer's DML configuration, credentials, database, or production services. Endpoint configuration and `DML_*` are hidden; normal `dml` and Python API usage discovers them. Getting started intentionally shows initialization against a disposable, initially uninitialized project. Page execution is ordered internally and independent of other pages; explicit fixture dependencies replace reliance on an earlier page's DAGs.

### Keep canonical scripts beneath docs/examples

Use `docs/` as the QMD source tree, preserving audience paths, with `docs/examples/<example>/` containing its lesson and canonical Python script(s). Populate executable Python cell bodies from those files through a build-time source inclusion step before knitr execution. The same unmodified files become downloads; no manually maintained copy of displayed code. Verify rendered source/download equivalence and execution source identity. Preserve file-backed source identity for Python inspection.

Include simpler Python workflows, excluding shell-oriented push/pull examples. Inventory Python examples individually rather than treating their file extension as proof of independence. Multiple-file examples provide individual downloads and a bundle preserving relative paths. Explain prerequisites through concise links, not exposed fixture code. Do not claim downloads initialize a user's environment automatically. Redundant Bash script-launch commands may be omitted; any displayed Bash commands still execute. Existing non-migrated shell workflows remain outside the docs collection and retain their own tests.

### Render static content into the persistent React shell

Render HTML content fragments with Quarto/Pandoc and no independent site navigation, Bootstrap shell, or runtime document scripts. Generate a static manifest for page titles, hierarchy, headings, asset paths, and download paths. The dashboard fetches and displays trusted packaged fragments inside a scoped docs content region using existing typography, themes, responsive layout, and navigation. Use static images/tables rather than interactive widget dependencies. No new package or browser-library dependency is required.

Use `/docs` and `/docs/<page>` as global browser routes, including `/docs/examples/<example>`. Relocate Swagger to `/api/docs`. Serve generated fragments/assets/downloads in a separate static namespace so SPA routes never collide with directory indexes. Validate manifest identifiers and static paths; missing content/downloads return an explicit not-found result rather than SPA HTML. Rewrite internal links and asset URLs at build time, preserve anchors/back-forward/deep links, and keep project revision scope out of docs navigation. Do not weaken existing frame, path-containment, or API authentication protections.

### Assemble only verified assets

Render into a clean staging directory, run validation, build Vite, then copy verified documentation into the final static tree after Vite's destructive output step. CI and both release packaging paths use the same orchestrated build entrypoint. Publication depends on successful execution and rendering; stale docs from earlier builds cannot satisfy the gate. Wheel/sdist checks verify nested content, assets, and downloads. Runtime startup requires none of the documentation build tools or fixture services.

## Risks / Trade-offs

- Reticulate source inspection differs from ordinary scripts -> prove representative script-executor/dagclass behavior before bulk migration.
- Shell failures can be masked by engine behavior -> negative integration tests plus explicit status-to-error handling.
- Full execution increases build time -> curate examples, share disposable services where safe, isolate page state; never cache away required execution.
- Fixture leakage or credential output -> disposable roots, fake credentials, bounded lifecycle, output inspection, and guaranteed cleanup.
- HTML/CSS or link collisions with the workbench -> script-free fragments, scoped styles, rewritten URLs, mobile/theme/navigation tests.
- R expands the build toolchain -> isolated pinned build tooling; unchanged published dependency metadata.

## Migration Plan

1. Prove engine/source compatibility and failure propagation; establish build-only fixture lifecycle.
2. Implement static docs routes and fragment rendering without changing project routes.
3. Convert docs in independently owned audience lanes after each owner reads relevant source/docs; migrate selected examples and update all maintained references, including other specs that explicitly name migrated `.md` paths.
4. Integrate the shared build into CI and release packaging; run source-policy, failure-injection, UI/server, and distribution checks.
5. Remove superseded migrated source copies only after canonical QMD/scripts work. Keep non-migrated example workflows intact. Rollback is a normal change revert/release rollback, not dual documentation sources or compatibility adapters.

## References Consulted

- `DOC_MAP.md`, `CONTRIBUTING.md`, `openspec/README.md`, `README.md`.
- `docs/index.qmd`, audience landing pages, getting-started, `docs/develop/architecture/dashboard.qmd`, `system-overview.qmd`, and security notes.
- Existing `human-facing-project-docs` and `dashboard-revision-navigation` specs.
- Quarto Execution Options (engine binding, shell commands, visibility), Using Python, and Using R documentation.
