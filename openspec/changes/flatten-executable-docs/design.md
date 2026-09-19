## Context

See `proposal.md` for motivation. Human pages currently occupy nested `concepts/`, `guides/`, `reference/`, and `develop/` directories, while dashboard navigation reconstructs a different hierarchy with hard-coded groups. `README.qmd` landing pages require a special render pass. Reader examples are split across QMD cells, orphaned `docs/examples/` downloads, and a top-level `examples/` integration suite. The dashboard job already runs the fail-closed documentation build.

## Goals / Non-Goals

**Goals:**

- Make authored QMD location, page ID, route, and navigation position agree.
- Keep the Start here lesson content, execution order, and shared project unchanged.
- Preserve useful Use and Extend content while removing document-type categorization.
- Preserve meaningful integration coverage before removing the example runner.
- Keep build implementation beside the docs while excluding it from published page discovery.

**Non-Goals:**

- Rewriting the Start here course.
- Defining a new content taxonomy to replace concepts, guides, and reference.
- Changing runtime, storage, extension, or dashboard product semantics.
- Publishing standalone example downloads.

## Decisions

### Use a strict flat human documentation tree

Authored human documentation consists only of `docs/start-here/*.qmd`, `docs/use/*.qmd`, `docs/extend/*.qmd`, `docs/glossary.qmd`, and `docs/sharp-bits-and-security.qmd`. Each section uses `index.qmd` as its landing page. Current nested pages move directly into Use or Extend; collisions receive descriptive topic names rather than another directory level.

Keeping the current category folders was rejected because the categories prescribe how each page must teach instead of reflecting the reader's destination. A single undifferentiated docs directory was rejected because Start here, Use, and Extend represent durable audience and sequence boundaries.

### Derive navigation from source paths

The build manifest records canonical page IDs from QMD paths, treating `index.qmd` as its containing route. Dashboard navigation renders Start here, Use, Extend, Glossary, and Sharp bits and security from those IDs without Concepts, Guides, Reference, Examples, Develop, or fallback grouping. The documentation root resolves to `start-here/index.qmd`.

This replaces hard-coded categorization and ordering maps. Explicit order remains only for the dependency-ordered Start here course; flat Use and Extend pages use stable manifest order.

### Keep Start here behavior unchanged

The four-step course, executable cells, page dependencies, and shared disposable project remain unchanged. Only references to removed documentation destinations may be redirected to root contributor guidance so the retained pages have no broken links.

### Put examples in the teaching pages

Runnable user and extension workflows live directly in the QMD page that explains them. Inline cells are preferred. A page may be terse or explanatory without changing its path or execution policy. Separate example pages, canonical download files, source-expansion markers, bundles, and packaged download inventories are removed.

The build continues to execute runnable cells without cache or failure suppression. Its current prohibition on executable Use pages is removed. Static code remains explicit pseudocode only when execution would misrepresent the example, not because of a page category.

### Consolidate the build entrypoint under docs

Root `build-dashboard.sh` and the current docs-only `docs/build.sh` are consolidated into one `docs/build.sh` entrypoint. It retains the existing build modes and orchestrates tool bootstrap, executable QMD rendering, dashboard UI compilation, staging, and packaging. Local guidance, CI, release jobs, fingerprints, and tests invoke that path directly. Supporting build scripts, lifecycle pages, pinned requirements, fixtures, and build-tool guidance remain under `docs/` but are excluded from human page discovery; the flat-tree constraint applies to published QMD content.

Contributor setup and testing consolidate in `CONTRIBUTING.md`; package, core, dashboard server, and dashboard UI orientation move to co-located READMEs. Dated investigations are removed or retained as historical OpenSpec material, not product docs.

### Preserve coverage by intent before deleting examples

Each top-level example behavior is classified before removal:

- Reader-worthy behavior moves into an executable Use or Extend page.
- Product contracts that would make docs slow, brittle, or incoherent move into integration tests.
- Hidden Moto, Docker, SSH, and dashboard fixture support moves to tooling or test fixtures.
- Dead or duplicate examples are deleted.

The standalone examples CI job is removed only after this mapping is complete. The existing dashboard job remains the single normal CI documentation gate; release builds continue to package verified docs.

## Risks / Trade-offs

- **Flattened filenames may collide or become vague** -> assign names by subject and validate route uniqueness.
- **Moving examples can reduce integration coverage** -> inventory behavior first and require a docs or test destination for every retained contract.
- **Executing more docs can increase build time or flakiness** -> keep reader workflows focused and place exhaustive infrastructure contracts in integration tests.
- **Removing download support can leave stale packaged files or routes** -> rebuild from a clean staging tree and assert the manifest contains no downloads or example routes.
- **Moving architecture prose can weaken discoverability** -> add concise co-located READMEs and update `DOC_MAP.md` and the OpenSpec authority overview.
- **Completed documentation changes describe the superseded structure** -> reconcile their current capability deltas before archival and treat this change as the final structural authority.

## Migration Plan

1. Establish the flat page inventory and destination name for every current Use, Extend, and Develop page.
2. Consolidate the complete build entrypoint at `docs/build.sh`; update callers, then move contributor architecture out of human docs and update maintainer maps and READMEs.
3. Flatten Use and Extend, convert landing pages to `index.qmd`, and repair links without changing Start here lessons.
4. Simplify page discovery, manifest IDs, navigation, and tests around the filesystem-derived tree.
5. Move or replace each top-level example and verify equivalent docs or integration coverage.
6. Remove example download/source-expansion support, the top-level examples tree, and the examples CI job.
7. Run documentation, dashboard, distribution, lint, type, and non-slow test verification from a clean build state.

Rollback restores the previous paths, navigation derivation, example runner, and packaged example support together; partial rollback would leave broken links or missing CI coverage.
