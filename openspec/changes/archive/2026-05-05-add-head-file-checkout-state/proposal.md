## Why

The current repository model conflates project identity, branch selection, and checkout state by deriving the active branch from config and environment inputs. That makes attached versus detached behavior implicit, keeps `HEAD` from being a real repository object, and leaves git-like project workflows harder to reason about than they need to be.

## What Changes

- Add a real `.dml/HEAD` file as the sole persisted source of local checkout state.
- Support two `HEAD` payload forms: `ref: refs/local/heads/<branch>` for attached mode and `commit:<id>` for detached mode.
- Change local project config so `[project].uri` is branchless project identity only: `dml://<owner>/<project>`.
- **BREAKING** Remove `DML_BRANCH` support entirely from runtime configuration, CLI behavior, hooks, and project workflows.
- **BREAKING** Remove config-derived current-branch behavior; commands that default to the current checkout MUST resolve it from `.dml/HEAD` instead of config.
- **BREAKING** Treat detached checkout state and tags as immutable sources: creating a child commit from a detached checkout does not move `HEAD` or any branch ref.
- Require mutable project workflows such as `push` and `pull` to operate only when `HEAD` is attached to a branch, with `push` defaulting to the attached branch's matching remote branch.
- Update revision resolution so `HEAD` and `HEAD~n` resolve through `.dml/HEAD` instead of an injected current-branch string.
- Explicitly reject backward-compatibility behavior: old branch-qualified local project config, `DML_BRANCH`, and mixed old/new checkout semantics are not supported by the new model.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `headops-pointer-management`: extend `HeadOps` to own persisted `.dml/HEAD` checkout state in addition to branch and index pointers.
- `shared-internal-configuration`: redefine project-local config so `project.uri` is branchless identity and branch selection is no longer a configuration concern.
- `git-like-commit-ops`: resolve checkout state from `.dml/HEAD`, formalize immutable detached commits, and require attached `HEAD` for mutable project workflows.
- `remote-project-refs`: remove `DML_BRANCH` and `[branch].current` assumptions from project-local config, init, hook environment, and project workflow defaults.
- `revision-parsing-contract-matrix`: update revision-resolution ownership so `HEAD` cases are defined by file-backed checkout state rather than config-derived branch context.

## Impact

- Affected code includes `_internal.config`, `HeadOps`, `CommitOps`, `IndexOps`, `DmlOps` project workflows, CLI project/status surfaces, and Python API default branch behavior.
- Existing repos using branch-qualified `.dml/config.toml` project URIs or relying on `DML_BRANCH` are incompatible with the new design unless manually rewritten outside any compatibility guarantee.
- Tests and docs covering checkout behavior, config precedence, revision parsing, init, hooks, and push/pull defaults must be updated to reflect the new repository model.
