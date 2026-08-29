## Context

The current CLI already has some git-like project verbs at the top level, but repository inspection still exposes internal storage nouns such as `commit`, `head`, `index`, `dag`, `node`, `cache`, and `remote` as the primary public interface. That makes common workflows harder to discover and couples the user model to implementation details instead of the repository concepts users care about: revisions, branch state, DAG maps, and administrative maintenance.

This redesign is intentionally a breaking CLI reset. The new surface keeps git-shaped porcelain at the top level, moves DAG inspection under `dml dag`, moves exceptional maintenance flows under `dml admin`, and standardizes all CLI outputs as JSON without changing any on-disk repository or remote storage formats.

The change is cross-cutting because it affects parser structure, CLI routing, JSON contracts, repository inspection entrypoints, DAG lookup flows, branch listing behavior, index reporting, and remote discovery/maintenance paths.

## Goals / Non-Goals

**Goals:**
- Present a coherent git-like top-level CLI for repository history and branch workflows.
- Make DAG inspection the first-class analogue to file inspection through `dml dag`.
- Isolate low-frequency maintenance flows under `dml admin`.
- Define stable JSON output contracts for the redesigned commands.
- Preserve thin CLI routing by moving orchestration and lookup behavior into non-CLI layers.
- Keep local and remote storage formats unchanged.

**Non-Goals:**
- Preserving any backward-compatible aliases, old command names, or legacy output payloads.
- Reworking the repository data model, HEAD file format, config file format, or remote CAS+refs layout.
- Introducing text-mode porcelain output that mimics git's terminal formatting.
- Exposing low-level remote push/pull plumbing commands as part of the new public CLI.
- Expanding cache management beyond explicit invalidation by exact cache key.

## Decisions

### Top-level porcelain is revision-centric rather than storage-centric

The public top-level surface will be `status`, `show`, `log`, `diff`, `checkout`, `branch`, `fetch`, `pull`, `push`, `merge`, and `revert`.

Rationale:
- This matches the mental model we want: users inspect repository history and branch state the way git users inspect trees and commits.
- It removes internal storage nouns from the common path.

Alternatives considered:
- Keep `commit`, `head`, and `index` public and add aliases. Rejected because the change is explicitly breaking and the old nouns would keep the split mental model alive.
- Move everything under subcommands. Rejected because top-level verbs are part of the git-like feel.

### `dml show` returns full DAG state plus commit delta

`dml show <revision>` will return top-level keys `revision`, `commit`, `dags`, and `change`.

`dags` is the complete DAG name-to-ref map for the resolved revision. `change` is the DAG-map delta introduced by that commit relative to its base commit.

Rationale:
- Users need both the complete tree picture and the specific change introduced by the commit.
- Keeping both in one payload avoids forcing a follow-up `dag list` call for context.

Alternatives considered:
- Return only the diff. Rejected because it omits the complete tree state.
- Nest the full tree under `tree`. Rejected in favor of promoting `dags` to a top-level field for clarity and directness.

### DAG inspection is organized by name-oriented lookups

`dml dag list` returns `dict[str, str]` for a revision. `dml dag get <name-or-id> [--revision REV]` returns a full DAG payload including node data.

When `dag get` receives an explicit `dag:<id>` selector, `--revision` is rejected. When it receives a plain name, the name is resolved against the DAG map for the selected revision.

Rationale:
- Most human workflows start from a DAG name in a revision tree, not a raw DAG ref.
- One `dag get` endpoint is enough if the payload includes node data.

Alternatives considered:
- Separate DAG metadata and node-inspection endpoints. Rejected because it keeps too much plumbing visible.
- Silently ignore `--revision` for `dag:<id>`. Rejected because it hides an invalid combination.

### Administrative workflows are isolated under `dml admin`

The admin surface will contain:
- `index list|get|delete`
- `cache invalidate <cache-key>...`
- `remote list [--owner OWNER]`
- `remote list dml://<owner>/<project>`
- `remote gc`
- `gc [--dry-run]`

Rationale:
- These are low-frequency maintenance or recovery flows, not normal repository inspection.
- Grouping them under `admin` keeps the main CLI focused while still exposing necessary escape hatches.

Alternatives considered:
- `runtime` for indexes/cache. Rejected because local GC and remote maintenance are not runtime state, and `admin` better matches the operational nature of the commands.
- Keep `remote` as a public top-level group. Rejected because user-facing sync remains `fetch`, `pull`, and `push`.

### Remote discovery uses one overloaded `admin remote list`

`dml admin remote list [--owner OWNER]` lists projects as canonical `dml://<owner>/<project>` URIs. `dml admin remote list dml://<owner>/<project>` lists remote branches and tags for that project.

Rationale:
- The overload follows the user's mental flow: list projects first, then inspect one project's remote refs.
- It avoids adding extra one-off verbs such as `list-projects` and `list-refs`.

Alternatives considered:
- Separate `list-projects` and `list-refs`. Rejected because it adds naming surface without more expressive power.

### Local and remote GC stay distinct

`dml admin gc` cleans local scratch-space storage and supports `--dry-run`. `dml admin remote gc` performs remote maintenance, including remote CAS/ref GC and remote transport cleanup, under one user-facing command.

Rationale:
- Local and remote cleanup have different stakes and should not be conflated.
- Remote prune and remote GC are implementation details that can be composed under one admin command.

Alternatives considered:
- One combined local+remote GC command. Rejected because users must never wonder whether a local cleanup also touched the remote.
- Separate remote `prune` and `gc`. Rejected because the user explicitly wants one remote maintenance command.

### Index reporting includes commit metadata, not just commit refs

`dml admin index list` returns indexes plus the commits they point to, and `dml admin index get` returns index state including full commit information rather than only a commit ref.

Rationale:
- Indexes are debugging/admin state. Returning the pointed-to commit metadata avoids immediate follow-up lookups and makes the admin commands useful on their own.

Alternatives considered:
- Return only commit ids. Rejected because it is too sparse for an admin inspection endpoint.

### Full config status moves to `dml config show`

`dml status` becomes repository/runtime status. Full resolved config output moves to `dml config show [--contrib]` and remains JSON.

Rationale:
- `status` should describe repository state in a git-shaped CLI.
- Config remains important, but it is a different concern.

## Risks / Trade-offs

- [Breaking CLI change] → Document the new command table clearly in specs, docs, and tests; do not preserve aliases that would muddy the new surface.
- [Cross-layer churn] → Introduce domain entrypoints for repository inspection and admin operations so the CLI remains thin.
- [Output contract drift] → Capture JSON payload shapes in specs and tests before implementation.
- [Remote list ambiguity] → Validate argument shape explicitly so project listing and per-project ref listing remain deterministic.
- [Remote GC scope confusion] → Keep `dml admin gc` and `dml admin remote gc` separate and document their different targets.
- [Admin command creep] → Limit `admin` to the locked set and defer additional plumbing commands unless a clear use case appears.
