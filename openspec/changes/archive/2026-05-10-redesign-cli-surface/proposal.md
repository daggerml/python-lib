## Why

The current `dml` CLI mixes git-like project verbs with storage-oriented plumbing commands, which makes common repository inspection feel inconsistent and exposes internal object boundaries as the primary user model. We want the CLI to read like git for repository history and branch workflows while making DAG inspection the first-class analogue to file inspection.

## What Changes

- **BREAKING** Replace the current top-level CLI surface with a git-shaped porcelain centered on `status`, `show`, `log`, `diff`, `checkout`, `branch`, `fetch`, `pull`, `push`, `merge`, and `revert`.
- **BREAKING** Replace storage-oriented inspection commands with DAG-oriented commands under `dml dag`, including `dag list`, `dag get`, `dag checkout`, and `dag delete`.
- **BREAKING** Move exceptional maintenance flows under `dml admin`, including index inspection/deletion, cache invalidation by cache key, remote discovery, remote GC, and local GC.
- Define stable JSON output contracts for the redesigned commands, including the locked `dml show` shape with top-level `revision`, `commit`, `dags`, and `change` keys.
- Redefine `dml status` as repository/runtime status instead of resolved config output, and move full config reporting to `dml config show [--contrib]`.
- Preserve thin CLI routing by keeping command handlers focused on parsing, delegation, and JSON serialization rather than embedding orchestration logic.

## Capabilities

### New Capabilities
- `repo-inspection-cli`: Git-shaped repository inspection and DAG inspection command contracts, including `show`, `status`, `log`, `diff`, `branch`, and `dag` output schemas.
- `admin-cli-controls`: Administrative command contracts for index management, cache invalidation, remote project discovery, and local/remote garbage collection.

### Modified Capabilities
- `cli-thin-interface`: Document the intentional CLI compatibility break while preserving the requirement that CLI modules remain thin transport adapters.
- `git-like-commit-ops`: Extend git-like repository workflows to cover branch listing semantics and revision-oriented inspection flows that power the new CLI surface.

## Impact

- Affects `src/daggerml/_cli/**`, `src/daggerml/_internal/ops/**`, CLI docs, and command contract tests.
- Changes the public CLI grammar and JSON payloads for existing commands.
- Requires new domain entrypoints for repository inspection, DAG lookup by revision/name, admin remote discovery, and richer index reporting.
- Keeps existing on-disk state formats and remote storage layout unchanged.
