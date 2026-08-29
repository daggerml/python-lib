## 1. Remote Project Model

- [x] 1.1 Add remote path helpers for `refs/projects/<owner>/<project>/heads/<branch>.json` and `refs/projects/<owner>/<project>/tags/<tag>.json`.
- [x] 1.2 Extend remote ref validation to accept project owner, project, branch, and tag path segments.
- [x] 1.3 Add branch head ref read/write support with ETag capture and conditional update.
- [x] 1.4 Add immutable project tag ref write support that rejects existing tag paths.
- [x] 1.5 Support canonical DML URI tracking refs for fetched remote branches and tags.
- [x] 1.6 Add explicit DML URI head validation, including canonicalization and the 64-byte URI limit.
- [x] 1.7 Keep existing cache and DAG ref behavior compatible with the shared CAS layout.

## 2. Local Config

- [x] 2.1 Define `.dml/` project directory layout for `.dml/config.toml`, local object database storage under `.dml/db/`, and `.dml/.gitignore`.
- [x] 2.2 Define global config resolution for `$DML_CONFIG_HOME`, `$XDG_CONFIG_HOME/dml`, and `~/.config/dml`.
- [x] 2.3 Define global config storage for `[user]`, `[defaults]`, and ordered `[hooks]` `post-init` and `post-clone` command lists.
- [x] 2.4 Define local config storage for required `[project]`, `[branch]`, and `[remotes.<name>]` fields.
- [x] 2.5 Implement config load/save helpers with validation for remote names, storage fields, and `dml://<owner>/<project>` URIs.
- [x] 2.6 Implement config waterfall resolution: explicit CLI/API argument, then supported environment variable, then config file value.
- [x] 2.7 Remove git-like project operation dependencies on obsolete env vars such as `DML_REPO`, `DML_REMOTE_ROOT`, `DML_DYNAMODB_TABLE`, and `DML_REMOTE_CACHE`.
- [x] 2.8 Add project creation behavior that defaults owner and branch from global config.
- [x] 2.9 Implement `dml init <name>` to create the project directory, `.dml/`, `.dml/config.toml`, `.dml/db/`, `.dml/.gitignore`, and initial branch commit.
- [x] 2.10 Implement `dml init --here <name>` to initialize the current directory while still running hooks unless `--no-hooks` is set.
- [x] 2.11 Implement `post-init` and `post-clone` shell hook execution with hook environment variables and `--no-hooks` support.

## 3. Branch Remote Operations

- [x] 3.1 Implement clone to create the project directory, initialize `.dml/`, initialize local state from a remote project branch, record `origin`, and run clone hooks.
- [x] 3.2 Implement fetch to materialize a configured remote branch and update local tracking ref `dml://<owner>/<project>#<branch>`.
- [x] 3.3 Implement explicit URI fetch for `dml://<owner>/<project>[identifier]` into a local remote-tracking head.
- [x] 3.4 Implement pull as fetch plus merge into the current/local branch.
- [x] 3.5 Implement push with closure upload, DML URI parsing into structured remote project paths, fast-forward ancestry validation, and ETag conditional branch-head update for existing branches.
- [x] 3.6 Implement push `--create` to create missing remote branches only when the ref does not already exist.
- [x] 3.7 Implement `--force` push to bypass fast-forward validation while still requiring ETag conditional update.

## 4. Commit and Head Operations

- [x] 4.1 Add head-advance/update operation for moving a branch head to a resolved commit.
- [x] 4.2 Update merge flow so user-facing merge advances the current head and fast-forwards when possible.
- [x] 4.3 Add structured merge conflict reporting for DAG-name conflicts.
- [x] 4.4 Implement commit revert by applying an inverse tree diff to the current branch as a new commit with safe-application conflict checks.

## 5. DAG Checkout

- [x] 5.1 Implement commit-ish resolution for commit refs, local heads, remote-tracking heads, `HEAD`, and first-parent `~N` syntax.
- [x] 5.2 Implement `dag checkout` tree update from source commit/name to target name with default overwrite refusal.
- [x] 5.3 Implement `--as` and `--replace` behavior for DAG checkout.
- [x] 5.4 Ensure checkout of an absent source DAG fails without deleting local DAG names.

## 6. CLI and Docs

- [x] 6.1 Add CLI commands for `init`, `clone`, `fetch`, `pull`, `push`, `merge`, `revert`, and `dag checkout`.
- [x] 6.2 Update remote, commit, DAG, CLI, and config documentation for the new behavior.
- [x] 6.3 Add tests for remote project ref paths, config resolution, fetch/pull/push safety, merge conflicts, revert, and DAG checkout.
- [x] 6.4 Run the relevant test suite and update any fixtures affected by remote layout changes.
