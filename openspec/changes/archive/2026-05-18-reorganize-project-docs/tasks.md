## 1. Establish the new docs skeleton and navigation

- [x] 1.1 Assign an independent subagent to inspect the current `docs/` tree, `README.md`, and the most central product entrypoints, then rewrite `docs/README.md` as a human-facing docs home with links to `getting-started`, concepts, guides, reference, architecture, and contrib.
- [x] 1.2 Have that same subagent create or align the top-level docs skeleton so the target sections exist with clear reader-facing purposes and no governance-style framing.
- [x] 1.3 Verify the new docs home explains the audience split clearly: `docs/` is for humans, `openspec/` is for change planning.

## 2. Rewrite the onboarding path as one concise getting-started page

- [x] 2.1 Assign an independent subagent to read the root `README.md`, current setup-related docs, and the public Python and CLI entrypoints, then write `docs/getting-started.md` as a single page covering installation, repo setup, first DAG creation, basic inspection, and next steps.
- [x] 2.2 Require that subagent to keep the page short and practical, using the real repo commands and API surface rather than generic onboarding prose.
- [x] 2.3 Verify the resulting page stands alone for a new reader without introducing a fragmented getting-started subtree.

## 3. Rebuild the core concepts lane

- [x] 3.1 Assign an independent subagent to inspect the current concept-heavy docs and the corresponding core modules, then map the content into target concept pages such as DAGs and nodes, commits and history, refs and namespaces, execution, storage, remotes, and codecs and values.
- [x] 3.2 Require that subagent to preserve the real technical model while rewriting the prose away from authority/invariant boilerplate and toward reader mental models.
- [x] 3.3 Verify the concepts lane explains how the major ideas fit together and avoids duplicating command-reference detail better suited to guides or reference docs.

## 4. Rebuild the guides lane

- [x] 4.1 Assign an independent subagent to inspect the current docs and likely workflows across the CLI and Python API, then draft task-oriented guides such as creating and running a DAG, inspecting a repository, working with remotes, storing external data, and troubleshooting common errors.
- [x] 4.2 Require that subagent to base each guide on real workflows already supported by the repo rather than aspirational flows.
- [x] 4.3 Verify the guides lane links outward to concepts and reference instead of trying to absorb all explanatory detail itself.

## 5. Rebuild the core reference lane

- [x] 5.1 Assign an independent subagent to inspect the public API, CLI, configuration, and error surfaces in code plus the current docs for those topics, then rewrite them as reader-facing reference docs under `docs/reference/`.
- [x] 5.2 Require that subagent to keep exactness where needed while avoiding spec-governance headings such as authority, handoffs, and compatibility sections.
- [x] 5.3 Verify the reference pages remain tightly aligned with the actual code surfaces and examples.

## 6. Rebuild the architecture lane

- [x] 6.1 Assign an independent subagent to inspect the internal modules, ops layer, storage implementation, remote protocol surface, and existing internal docs, then reshape them into architecture docs under `docs/architecture/`.
- [x] 6.2 Require that subagent to explain subsystem relationships, data flow, and layering in human terms while staying grounded in the real module layout.
- [x] 6.3 Verify the architecture lane serves advanced readers and contributors without reverting to a normative spec voice.

## 7. Rebuild contrib docs as a parallel human-facing subtree

- [x] 7.1 Assign an independent subagent to inspect `src/daggerml/contrib/**`, the existing `docs/contrib/` set, and any relevant examples/tests, then reorganize contrib docs into a coherent human-facing subtree with a clear start point plus concepts, guides, reference, and architecture sections as needed.
- [x] 7.2 Require that subagent to preserve contrib-specific runtime and API detail while matching the tone and reader-intent model of the main docs.
- [x] 7.3 Verify contrib docs are navigable on their own and clearly connected back to the main docs home.

## 8. Move maintainer workflow material out of `docs/`

- [x] 8.1 Assign an independent subagent to inspect `docs/DOC_MAP.md`, `docs/spec/overview.md`, `docs/testing-taxonomy.md`, `AGENTS.md`, `CONTRIBUTING.md`, and `.opencode/`, then propose and execute new homes for maintainer- and agent-facing guidance outside `docs/`.
- [x] 8.2 Require that subagent to preserve the workflow value of those documents while making their audience explicit in their new locations.
- [x] 8.3 Verify the final `docs/` tree no longer contains maintainer workflow rules or spec-governance material.

## 9. Final coherence pass

- [x] 9.1 Assign an independent subagent to review the completed docs tree as a reader, checking navigation, cross-links, tone consistency, and audience boundaries across all lanes.
- [x] 9.2 Require that subagent to spot duplicated material, stale links, and sections that still read like internal specs instead of human docs.
- [x] 9.3 Verify the final result presents DaggerML clearly as it exists today and that each lane is grounded in actual repo behavior.
