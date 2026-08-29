## Context

The current repository has strong technical documentation coverage, but most of that coverage is written as an authority-driven spec suite. `docs/README.md` is a governance index, `docs/DOC_MAP.md` is an edit workflow rulebook, `docs/spec/overview.md` is a concept-authority map, and most topic docs lead with status, scope, authority, invariants, and compatibility language. That structure is useful for disciplined maintenance, but it does not match the audience boundary we want now: `docs/` should be for humans trying to understand or use DaggerML as it exists.

The repo also already has a natural place for agent-facing planning and change management: `openspec/`. That lets us make a cleaner distinction:

- `docs/`: human-facing project docs
- `openspec/`: change proposals, specs, and tasks for agent-driven work
- contributor workflow docs outside `docs/`: edit pre-read rules, test taxonomy, and similar maintainer guidance

This change is a documentation-architecture change rather than a product behavior change. The risk is not code regression; the risk is losing useful technical information, preserving the wrong audience voice, or turning the work into a path reshuffle without improving the reading experience.

## Goals / Non-Goals

**Goals:**
- Establish a human-facing information architecture for `docs/`.
- Keep `getting-started` as one concise page rather than a directory of tiny setup docs.
- Define what each doc lane contains: docs home, getting started, concepts, guides, reference, architecture, and contrib.
- Identify which current docs stay in `docs/` as rewritten material and which should move out because they are maintainer- or agent-facing.
- Require each implementation task to be handled by an independent subagent that first reads the repo and relevant code/docs so the rewritten docs remain grounded in reality.
- Preserve dense technical knowledge while translating it into reader-first prose.

**Non-Goals:**
- Changing code behavior, API semantics, CLI semantics, or storage/runtime behavior.
- Rewriting every sentence in one pass without regard to existing useful content.
- Moving OpenSpec change artifacts into `docs/` or treating `docs/` as a normative spec suite.
- Creating a large multi-file onboarding section for basics that fit comfortably in one getting-started document.

## Decisions

### `docs/` will be organized by reader intent

The target top-level shape will be:

- `docs/README.md`: reader-facing docs home and navigation map
- `docs/getting-started.md`: one compact setup-and-first-success page
- `docs/concepts/`: mental models and core domain explanations
- `docs/guides/`: task-oriented walkthroughs
- `docs/reference/`: API, CLI, configuration, and error reference material
- `docs/architecture/`: system structure and internal design explanations for advanced readers and contributors
- `docs/contrib/`: a parallel subtree for contrib-specific concepts, guides, reference, and architecture

Rationale:
- Human readers usually navigate by intent such as learning concepts, accomplishing a task, or checking a reference.
- The current source-tree-shaped and authority-shaped layout leaks maintainer concerns into the main reading path.

Alternatives considered:
- Keep the existing file layout and only soften the language. Rejected because the structure itself still centers governance instead of reader needs.
- Mirror the source tree directly in docs. Rejected because implementation decomposition is not the best primary reading experience.

### `getting-started` stays a single file

`docs/getting-started.md` will be one concise page that covers:

- what DaggerML is in a few sentences
- installation as a one-line `pip install daggerml`
- optional CLI installation note
- creating a repo
- creating a first DAG in Python
- one or two CLI inspection commands such as listing DAGs
- cleanup or next-step links

Rationale:
- The basic setup path is short and should feel fast.
- Splitting installation, first repo, and first DAG into many files adds navigation overhead without adding conceptual value.

Alternatives considered:
- A `getting-started/` directory with several pages. Rejected because the setup surface is too small to justify a subtree.

### Existing project docs will be rewritten into four human-facing doc modes

Current technical material will be translated into one of four doc modes:

- Concepts: explain what something is and how to think about it
- Guides: explain how to accomplish a workflow
- Reference: explain exact user-facing surfaces and options
- Architecture: explain how the internals are structured and interact

Likely placement of current material:

- `api.md` -> `reference/python-api.md`
- `cli.md` -> `reference/cli.md`
- `configuration.md` -> `reference/configuration.md`
- `errors.md` -> `reference/errors.md`
- `object-model.md`, `dag-model.md`, `commit-model.md`, `codec-system.md`, `execution-model.md`, `remote-sync.md` -> `concepts/`
- `system.md`, `internal/README.md`, `internal/ops/*.md`, `remote-protocol.md`, deep internal storage/type docs -> `architecture/`
- `storing-and-retrieving-external-data.md` may split into a concept doc plus a guide if the current material mixes model and workflow

Rationale:
- Most current docs contain valuable content, but their framing is wrong for the intended audience.
- Separating mode by reader question makes each page easier to write and easier to use.

Alternatives considered:
- Preserve file names and just change headings. Rejected because many names and paths currently encode implementation ownership rather than reader purpose.

### Maintainer governance docs will move out of `docs/`

The following categories will no longer live in human-facing project docs:

- edit pre-read workflow maps such as `docs/DOC_MAP.md`
- spec-governance indexes such as `docs/spec/overview.md`
- contributor policy docs such as `docs/testing-taxonomy.md`

These should move to contributor or agent-facing homes such as `CONTRIBUTING.md`, `AGENTS.md`, `.opencode/`, or another clearly maintainer-oriented location chosen during implementation.

Rationale:
- These documents describe how maintainers and agents work on the repo, not how DaggerML works.
- Leaving them under `docs/` blurs the audience boundary the change is trying to create.

Alternatives considered:
- Keep them in `docs/` under a maintainer-only subdirectory. Rejected because the user explicitly wants `docs/` to be the human-facing project docs set.

### Each doc lane should have named target pages and concrete content expectations

The reorganization will not stop at creating directories. Each target area should have a defined purpose and likely page set.

Proposed contents:

- `docs/README.md`
  - explain the overall docs map
  - link readers to getting started, concepts, guides, reference, architecture, and contrib
  - explain in one short note that `openspec/` is for change planning, not product docs

- `docs/getting-started.md`
  - installation
  - create/select a repo
  - create a first DAG in Python
  - inspect with CLI
  - pointers to next concept and reference docs

- `docs/concepts/`
  - `overview.md`: how the concepts fit together
  - `dags-and-nodes.md`
  - `commits-and-history.md`
  - `refs-and-namespaces.md`
  - `execution.md`
  - `storage.md`
  - `remotes.md`
  - `codecs-and-values.md`

- `docs/guides/`
  - `create-and-run-a-dag.md`
  - `inspect-a-repository.md`
  - `work-with-remotes.md`
  - `store-and-load-external-data.md`
  - `troubleshoot-common-errors.md`

- `docs/reference/`
  - `python-api.md`
  - `cli.md`
  - `configuration.md`
  - `errors.md`

- `docs/architecture/`
  - `system-overview.md`
  - `internal-modules.md`
  - `ops-layer.md`
  - `storage-internals.md`
  - `remote-protocol.md`
  - `type-system.md`

- `docs/contrib/`
  - `README.md`
  - `getting-started.md` or a short start section inside the README if the material is small
  - `concepts/`
  - `guides/`
  - `reference/`
  - `architecture/`

Rationale:
- Named target pages make the work concrete and reviewable.
- Writers can preserve and reshape existing material with much less ambiguity.

Alternatives considered:
- Leave page selection to whoever implements each subtree. Rejected because that would create inconsistent granularity and duplicated topics.

### Implementation work will be partitioned into independent repo-aware subagents

The task plan will assign each major docs area to a separate subagent. Each subagent must inspect the current repo before drafting docs for its area, including:

- the current docs that cover the same topic
- the relevant source modules and entrypoints
- the root `README.md` and contributor context where relevant

Subagents should be able to work in parallel because the new IA boundaries are intentionally separated by reader intent and subtree ownership.

Rationale:
- Good docs require understanding the real system, not just moving prose around.
- Independent subagents reduce merge contention and allow parallel progress while keeping each lane coherent.

Alternatives considered:
- One agent rewrites the entire docs tree. Rejected because it couples unrelated doc lanes and makes it harder to maintain area-specific grounding.

## Risks / Trade-offs

- [Useful technical detail gets lost during simplification] -> Preserve existing dense docs as source material and require each subagent to read both current docs and relevant code before rewriting.
- [New IA creates dead links or duplicated explanations] -> Define clear page purposes up front and reserve overview pages for navigation rather than repeated deep content.
- [Contrib docs drift away from core docs style] -> Give contrib its own parallel subtree but require the same concepts/guides/reference/architecture split.
- [Maintainer workflow docs become harder to find after leaving `docs/`] -> Choose explicit destination homes during implementation and update contributor-facing entry points at the same time.
- [Subagents write generic docs disconnected from the codebase] -> Require repo inspection in every task and review outputs for code-anchored accuracy.

## Migration Plan

1. Create the new docs skeleton and human-facing docs home.
2. Rewrite the root getting-started experience as a single file.
3. Migrate core content into concepts, guides, reference, and architecture lanes.
4. Rebuild `docs/contrib/` using the same reader-intent model.
5. Move maintainer workflow docs out of `docs/` and update their entry points.
6. Remove or redirect obsolete paths and verify the final navigation is coherent.

Rollback is low-risk because this is documentation-only. If the new shape proves confusing during review, files can be reworked before merge without product-facing compatibility concerns.

## Open Questions

- Should contributor-only docs outside `docs/` live primarily in `CONTRIBUTING.md`, in a dedicated contributor-docs subtree, or in `.opencode/` and `AGENTS.md` depending on audience?
- Should `docs/contrib/` include its own separate getting-started page, or is a short reader path in `docs/contrib/README.md` enough?
- Which current concept docs need to split into both a concept page and a guide, instead of being translated one-to-one?
