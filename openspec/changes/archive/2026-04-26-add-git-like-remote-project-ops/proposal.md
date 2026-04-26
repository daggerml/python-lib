## Why

DaggerML already has commits, heads, trees, and S3-backed CAS sync, but the remote UX does not yet behave like git for shared project branches. Users need a clear project/branch remote model with safe `clone`, `fetch`, `pull`, `push`, `merge`, and DAG checkout operations.

## What Changes

- Add a remote project namespace layout: `refs/projects/<owner>/<project>/{heads,tags}/`.
- Add mutable remote branch heads and immutable project tags for git-like branch discovery and stable releases.
- Store fetched remote branches/tags locally under canonical normalized `dml://<owner>/<project>#<branch>` or `dml://<owner>/<project>@<tag>` URIs.
- Add global DML config under `$DML_CONFIG_HOME`, `$XDG_CONFIG_HOME/dml`, or `~/.config/dml` for user defaults and init/clone hooks.
- Add project-local config under `.dml/config.toml` for project identity and named remotes such as `origin`, enabling commands like `dml push origin main`.
- Define `dml init <name>` and `dml clone` project-directory initialization, including `<project-directory>/.dml/` config, database storage, `.dml/.gitignore`, and optional shell hooks for user project setup commands.
- Define git-like command semantics for `dml clone`, `dml fetch`, `dml pull`, `dml push`, `dml merge`, and `dml revert`, including explicit URI fetches such as `dml fetch dml://<owner>/<project>[identifier]`.
- Define `dml dag checkout <commit-ish> <dag-name> [--as <name>] [--replace]` for copying one DAG from another commit into the current branch as a new commit.
- Require push safety checks: conditional remote head update by ETag and fast-forward-only ancestry unless `--force` is specified.
- Keep `--force` subject to ETag checks to prevent lost-update races.

## Capabilities

### New Capabilities

- `remote-project-refs`: Remote project namespace, branch/tag ref layout, local remote configuration, and safe branch push/fetch/pull behavior.
- `git-like-commit-ops`: User-facing merge, revert, and DAG checkout operations that create commits and advance heads.

### Modified Capabilities

- None.

## Impact

- Remote data model and protocol docs/specs for project refs, DML URI local tracking refs, and branch-head mutation.
- Internal remote ops for project-aware branch heads, immutable tags, conditional updates, fetch/pull/push, and clone.
- Commit/head ops for head-advancing merge/revert and DAG checkout.
- CLI commands for git-like branch operations and DAG checkout.
- Global DML configuration for user defaults and bootstrap hooks.
- Local repository configuration for project identity and named remotes.
- Local project initialization hooks for commands such as `uv init`.
