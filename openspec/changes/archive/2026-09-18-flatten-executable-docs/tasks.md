## 1. Inventory And Destinations

- [x] 1.1 Record the flat `docs/use/*.qmd` and `docs/extend/*.qmd` destination for every current page, including deliberate names for collisions.
- [x] 1.2 Map every top-level `examples/` workflow and support file to an executable docs page, an integration test or fixture, or deletion, with no retained behavior left unclassified.
- [x] 1.3 Identify the reader-facing content in `docs/develop/` that must be preserved in Use or Extend and the contributor-only content that must move to maintainer READMEs.

## 2. Build And Maintainer Material

- [x] 2.1 Consolidate root `build-dashboard.sh` and the current docs-only script into `docs/build.sh`, preserving build modes, tool bootstrap, rendering, UI compilation, staging, and packaging.
- [x] 2.2 Consolidate contributor setup and testing guidance in `CONTRIBUTING.md` and create concise co-located READMEs for the Python package, core, dashboard server, and dashboard UI.
- [x] 2.3 Update local guidance, CI and release callers, fingerprints, tests, `DOC_MAP.md`, root links, and `openspec/spec-overview.md` for `docs/build.sh`, the new QMD paths, co-located READMEs, and current OpenSpec authorities.
- [x] 2.4 Remove the dated Develop investigation after confirming its durable outcomes remain in tests, archived changes, or current specs.

## 3. Flat Human Documentation

- [x] 3.1 Move all retained researcher pages directly beneath `docs/use/`, merge duplicate coverage, and replace Use `README.qmd` files with one `docs/use/index.qmd`.
- [x] 3.2 Move all retained integration pages directly beneath `docs/extend/`, merge duplicate coverage, and retain one `docs/extend/index.qmd`.
- [x] 3.3 Remove `docs/develop/` and `docs/examples/`, including orphan downloads, after their retained content reaches its assigned destination.
- [x] 3.4 Repair all internal and repository links while preserving the Start here lesson code, dependency graph, execution order, and shared project behavior.
- [x] 3.5 Ensure authored human documentation contains only `start-here/`, flat `use/`, flat `extend/`, `glossary.qmd`, and `sharp-bits-and-security.qmd`.

## 4. Executable Docs And Example Coverage

- [x] 4.1 Remove category-based execution restrictions so runnable Use and Extend examples execute under the existing fail-closed policy.
- [x] 4.2 Move reader-worthy CLI, cache invalidation, runtime inspection, cancellation, remote sync, Docker, SSH, freeze, and extension workflows into their teaching pages with focused assertions.
- [x] 4.3 Add or relocate integration tests for exhaustive or infrastructure-heavy contracts that should not appear as reader workflows.
- [x] 4.4 Relocate Moto, Docker, SSH, dashboard demo, and plugin fixture support to build tooling or test fixtures and update their contract tests.
- [x] 4.5 Remove source-expansion markers, example ZIP/download staging, download manifest fields, and tests that require packaged standalone examples.
- [x] 4.6 Delete the top-level `examples/` tree after verifying every classified dependency and behavior has reached its destination.

## 5. Path-Derived Documentation UI

- [x] 5.1 Normalize `index.qmd` page IDs and remove the special `README.qmd` render path while retaining dependency-ordered Start here execution.
- [x] 5.2 Generate manifest routes and navigation metadata directly from flat QMD paths, with `/docs` resolving to `start-here/index.qmd`.
- [x] 5.3 Replace dashboard hard-coded Concepts, Guides, Reference, Examples, Develop, and fallback groups with Start here, Use, Extend, Glossary, and Sharp bits and security.
- [x] 5.4 Update browser, server, manifest, link, packaged-asset, and distribution tests for the filesystem-mirrored routes and absence of example downloads.

## 6. CI And Verification

- [x] 6.1 Remove the standalone examples CI job and its publish dependency after executable docs and integration tests cover the classified behavior.
- [x] 6.2 Reconcile the completed executable-documentation changes with this superseding structure so their specs can be archived without restoring Develop or Examples requirements.
- [x] 6.3 Run OpenSpec validation, documentation policy and build tests, dashboard UI/server tests, and installed-distribution checks.
- [x] 6.4 Run the full clean executable dashboard build and required Ruff, Pyright, and non-slow pytest checks.
