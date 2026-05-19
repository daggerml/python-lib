## Why

The current `docs/` tree mixes human-facing project documentation with maintainer governance, edit workflow rules, and spec-suite authority language. We want `docs/` to read as a description of DaggerML as it exists for human readers, while keeping agent-facing change planning in `openspec/` and moving maintainer workflow material out of the project docs surface.

## What Changes

- Reorganize `docs/` around reader intent instead of authority ownership: a docs home, one concise getting-started page, concept docs, guides, reference docs, architecture docs, and a parallel `contrib` docs subtree.
- Rewrite the project docs so they describe DaggerML in user-facing language rather than normative spec language such as document authority, compatibility classes, and mandatory handoff rules.
- Keep `getting-started` as a single compact page that covers installation, first repo, first DAG, and where to go next instead of splitting those basics across multiple tiny files.
- Define specific content expectations for each target doc so the reorganization is not just path churn.
- Move maintainer- and agent-facing material out of `docs/`, including edit pre-read workflow guidance and spec-governance content that belongs with contributor or agent tooling instead.
- Preserve valuable technical content by translating existing docs into concept, reference, and architecture narratives rather than deleting detail.

## Capabilities

### New Capabilities
- `human-facing-project-docs`: Define the required information architecture and audience boundary for the repository's human-facing project documentation.

### Modified Capabilities

None.

## Impact

- Affects `docs/README.md`, most existing `docs/*.md` files, and the organization of `docs/internal/` and `docs/contrib/` content.
- Affects maintainer-oriented docs that currently live under `docs/`, especially `docs/DOC_MAP.md`, `docs/spec/overview.md`, and `docs/testing-taxonomy.md`.
- Does not change runtime behavior, public APIs, CLI semantics, storage formats, or OpenSpec capability behavior.
- Changes how contributors and readers discover project information, so the final docs need clear navigation and consistent audience boundaries.
