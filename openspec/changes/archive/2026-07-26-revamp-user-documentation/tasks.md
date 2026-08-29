## 1. Inventory And Information Architecture

- [x] 1.1 Inventory every current `docs/` page, relevant example, and root documentation link; map each to a Use, Extend, Develop, maintainer-outside-docs, redirect, or removal destination.
- [x] 1.2 Inspect the current public CLI, Python authoring APIs, extension surfaces, and contributor guidance to validate the planned page scopes and command examples.
- [x] 1.3 Create the new `docs/` directory skeleton, audience landing pages, shared glossary, and navigation conventions for Use, Extend, and Develop.
- [x] 1.4 Write the root docs home and Why DaggerML page, including the durable-research rationale, good-fit cases, and non-goals.

## 2. Researcher Documentation

- [x] 2.1 Rewrite the researcher getting-started path to install DaggerML, create/configure a project with `dml init`, author a first DAG in Python, and inspect the result with the CLI.
- [x] 2.2 Write researcher concepts for research projects, DAGs/nodes/results, funks/execution, runtimes, caching, artifacts/data/codecs, history/remotes, and errors/provenance.
- [x] 2.3 Write researcher guides for DAG authoring, funks, Docker-backed work, supported remote execution, external data, custom codecs, temporary DML projects, runtime inspection/cancellation, cache refresh, failure inspection, and research sharing/reuse.
- [x] 2.4 Create curated researcher CLI, Python authoring, configuration, runtime-state, and error references; document low-level `Dml.init(...)` without presenting it as the normal project-creation workflow.
- [x] 2.5 Verify every researcher example uses the CLI for project administration, Python for research authoring, and runtime rather than index as the primary user term.

## 3. Extension Documentation

- [x] 3.1 Write the Extend DaggerML landing page and concepts for the extension model, adapters/executors, codecs, remote integrations, and plugin registration.
- [x] 3.2 Write integration-engineer guides for adapters, executors, shared codecs, packaging, and testing after inspecting the corresponding source modules and existing examples.
- [x] 3.3 Write extension references for adapter operations, executor lifecycle, codec contracts, plugin APIs, and built-in integrations.
- [x] 3.4 Move or translate extension material from `docs/contrib/` without making `contrib` a primary navigation category.

## 4. Contributor Documentation

- [x] 4.1 Write the Develop DaggerML landing page, development setup, testing guide, codebase map, and stable contributor-facing architecture pages.
- [x] 4.2 Separate stable contributor documentation from agent instructions, OpenSpec governance, edit maps, and other maintenance-policy documents that remain outside `docs/`.
- [x] 4.3 Verify the Develop path is discoverable for contributors but is not part of the researcher or integration-engineer learning sequence.

## 5. Migration And Validation

- [x] 5.1 Update the repository README and all surviving internal documentation links to the new root and audience paths.
- [x] 5.2 Remove obsolete top-level docs taxonomy and `docs/contrib/` only after every retained technical topic has a validated replacement or intentional external destination.
- [x] 5.3 Assess redirect support and published-path migration needs; the repository has no documentation-host configuration or redirect mechanism, so no redirect or stub is added.
- [x] 5.4 Validate Markdown links, target tree, CLI commands, Python snippets, terminology, and code examples against the repository; record any unavailable integration prerequisites explicitly.
