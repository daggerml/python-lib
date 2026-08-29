## Why

Git-like project workflows are currently split between CLI handlers and internal ops, which makes command behavior harder to maintain and test consistently. We need a single internal operation entrypoint for these workflows so the CLI remains a thin parser/dispatcher layer.

## What Changes

- Move git-like project command orchestration (`fetch`, `pull`, `push`, `checkout`, `merge`, `revert`, and `clone` composition logic) behind `DmlOps` methods.
- Reduce `src/daggerml/_cli/` command handlers to argument parsing, basic input normalization, and calling one `DmlOps` method per command.
- Keep user-visible command semantics and error surfaces aligned with current git-like behavior while relocating implementation ownership.
- Add or update tests to enforce thin CLI routing and `DmlOps` ownership for git-like project operations.

## Capabilities

### New Capabilities
- `thin-cli-routing`: Define a requirement that git-like project command handlers in `_cli` are thin wrappers that delegate operational behavior to `DmlOps`.

### Modified Capabilities
- `git-like-commit-ops`: Clarify that git-like project operations are executed by internal ops-owned methods surfaced via `DmlOps`, with CLI acting as a transport layer.

## Impact

- Affected code: `src/daggerml/_cli/project.py`, `src/daggerml/_cli/base.py`, and `src/daggerml/_internal/ops/__init__.py` (plus any extracted internal ops helpers).
- Affected tests: CLI project command tests and internal ops tests around git-like flows.
- API impact: no new end-user CLI flags required; behavior remains compatible while ownership shifts to internal ops.
