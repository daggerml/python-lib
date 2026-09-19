## Why

The flat Use and Extend documentation paths expose too many narrowly divided pages, repeat contracts across concept, guide, and reference documents, and make readers assemble workflows themselves. Consolidating each path into a small ordered executable course will make the dashboard navigable while preserving technical depth and turning the documented journeys into build-verified integration coverage.

## What Changes

- Replace the current Use page inventory with an ordered six-page course: Projects, Artifacts, Execution, Inspection, Runtimes, and Sharing.
- Continue the Use course from the completed Start here project, declare page-to-page prerequisites, and make the primary workflows execute against durable state produced by preceding pages.
- Replace the current Extend page inventory with an ordered three-page course: Codecs, Adapters, and Executors.
- Make the Extend course follow one coherent integration journey, with each page combining its mechanism's concepts, public contracts, implementation guidance, registration, packaging, examples, and testing.
- Fold useful content from superseded concept, guide, and reference pages into the new course pages; move researcher, extension, dashboard, security, glossary, contributor, and core-architecture material to the audience that owns it.
- Remove redundant Use and Extend landing/reference pages from dashboard navigation instead of preserving them as forwarding pages.
- Make dashboard navigation use the validated documentation dependency order for Start here, Use, and Extend.
- Keep fixture-owned workflows executable and assertion-backed while explicitly marking integrations that require unowned external infrastructure as pseudocode.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `researcher-documentation`: Replace the broad flat Use catalog with a six-page ordered, executable continuation of Start here.
- `extension-documentation`: Replace the fragmented Extend catalog with three ordered, executable mechanism journeys.
- `human-facing-project-docs`: Apply one validated dependency order to build execution and dashboard navigation for all three course sections, and preserve technical content while removing superseded pages.

## Impact

- Documentation sources under `docs/use/`, `docs/extend/`, and cross-links from Start here, the glossary, Sharp bits and security, contributor docs, and subsystem READMEs.
- Documentation dependency metadata, executable build fixtures, and build validation tests.
- Dashboard documentation navigation and frontend tests that currently order only Start here by manifest position.
- Packaged documentation routes and links: removed leaf routes are intentionally consolidated rather than retained as duplicate compatibility pages.
- No DaggerML runtime, storage, CLI, or Python API behavior changes.
