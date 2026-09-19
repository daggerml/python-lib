## Why

The documentation is split by premature content categories and duplicates worked examples in a separate source tree and CI job. A smaller filesystem-driven structure can make the documentation itself the executable example suite while keeping contributor material beside the code it describes.

## What Changes

- **BREAKING**: reduce the human documentation tree to `start-here/`, flat `use/`, flat `extend/`, `glossary.qmd`, and `sharp-bits-and-security.qmd`.
- Preserve the existing Start here course, its order, and its executable lessons.
- Remove the Concepts, Guides, Reference, Develop, and Examples navigation categories and their corresponding directory structure.
- Move useful Use and Extend pages directly beneath their audience directory, resolving filename collisions deliberately rather than retaining category folders.
- Make reader-facing documentation the canonical executable examples and remove separate example downloads and example-only routes.
- Move contributor workflow and subsystem orientation into repository and co-located README files; keep normative behavior in OpenSpec.
- Replace the standalone examples CI gate with the existing executable documentation/dashboard build after preserving appropriate coverage in docs or tests.
- Move the complete dashboard-and-documentation build entrypoint from root `build-dashboard.sh` to `docs/build.sh`, folding in the current docs-only script and updating all local, CI, and release callers.
- Remove the top-level `examples/` tree after relocating required build fixtures, integration coverage, and reader-facing material.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `human-facing-project-docs`: Replace document-type and contributor/example sections with a filesystem-mirrored Start here, Use, and Extend structure.
- `researcher-documentation`: Make flat Use pages teach and verify researcher workflows directly through examples.
- `extension-documentation`: Make flat Extend pages teach and verify adapter, executor, codec, plugin, and integration authoring directly.
- `contributor-documentation`: Move contributor guidance out of human product docs and into repository and co-located README files.

## Impact

This affects documentation paths and links, Quarto discovery and page IDs, dashboard navigation and packaged docs, the build entrypoint and its tests, contributor READMEs and `DOC_MAP.md`, example fixtures and integration tests, CI/release dependencies, and the current documentation OpenSpec authority map. It does not change DaggerML runtime or storage behavior.
