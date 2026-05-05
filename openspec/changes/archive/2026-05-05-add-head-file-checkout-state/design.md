## Context

The current repository model splits branch state across mutable branch refs, branch-qualified `project.uri`, and runtime/config overrides. That leaves `HEAD` as a caller-provided concept instead of a persisted repository object, makes detached behavior only partially modeled, and lets mutable project workflows derive their target branch from configuration rather than checkout state.

This change replaces that model with a Git-like repository truth boundary:

- `.dml/config.toml` stores branchless project identity and runtime defaults.
- `.dml/HEAD` stores the current checkout state.
- `.dml/refs/local/heads/<branch>` stores mutable local branch tips.
- Detached commits and tags remain immutable commit selectors.

This is an intentional breaking change. The design does not preserve compatibility with branch-qualified local `project.uri`, `DML_BRANCH`, or mixed old/new checkout semantics.

## Goals / Non-Goals

**Goals:**

- Make `.dml/HEAD` the sole persisted source of local checkout state.
- Separate project identity from branch selection by making local `project.uri` branchless.
- Make attached and detached checkout semantics explicit and testable.
- Require mutable project workflows to operate only from an attached local branch.
- Keep the Python API override surface available while making default repository behavior depend on `.dml/HEAD`.
- Remove all backward-compatibility paths for the old branch/config/env model.

**Non-Goals:**

- Preserving compatibility with repositories that still use branch-qualified local config.
- Supporting detached-HEAD branch advancement semantics.
- Allowing `push` or `pull` to mutate history from detached HEAD.
- Introducing a migration shim, auto-upgrade path, or dual-read support for old and new config formats.

## Decisions

### `.dml/HEAD` is a plain-text repository object owned by `HeadOps`

`HeadOps` already owns file-backed pointer persistence, so it will also own the checkout-state file. The persisted `HEAD` payload has exactly two valid forms:

- `ref: refs/local/heads/<branch>`
- `commit:<id>`

This keeps checkout state explicit, human-readable, and aligned with the branch/index pointer boundary already managed by `HeadOps`.

Alternative considered:

- Store a structured TOML/JSON HEAD object. Rejected because the state machine only needs two payload forms and plain text keeps parsing and manual inspection simpler.

### Local project config becomes branchless identity only

`[project].uri` in local config becomes `dml://<owner>/<project>` with no branch or tag selector. Branch selection is no longer a configuration concern. `default_branch` remains a bootstrap and fallback selector for commands that need a branch when creating or fetching initial state, but it no longer represents the active checkout.

Alternative considered:

- Keep branch-qualified `project.uri` and add `.dml/HEAD` on top. Rejected because it preserves two competing sources of truth for the active branch and leaves detached semantics ambiguous.

### Detached commits are immutable sources and do not advance `HEAD`

When `.dml/HEAD` contains `commit:<id>`, commands that create detached commits may materialize child commits, but `HEAD` remains unchanged and no branch ref moves. This matches the existing low-level `IndexOps.commit(head=None)` behavior and makes detached state a read/derive surface rather than a mutable line of development.

Alternative considered:

- Advance `.dml/HEAD` to the newly created detached commit. Rejected because it would make detached state partially mutable, blur the distinction between branches and immutable selectors, and complicate default push/pull semantics.

### `HEAD` resolution moves from injected branch context to repository state

Revision resolution will treat `HEAD` and `HEAD~n` as repository-state expressions backed by `.dml/HEAD`. Resolver entry points that currently depend on a caller-supplied current branch will instead resolve `HEAD` from the repository and then walk ancestry from the resolved commit.

Alternative considered:

- Keep `HEAD` as syntactic sugar for a caller-provided branch name. Rejected because it leaves `HEAD` disconnected from checkout state and breaks detached resolution.

### Mutable project workflows require attached HEAD

Project workflows that mutate a branch, especially `push` and `pull`, require `.dml/HEAD` to be attached to a local branch unless the command explicitly targets a mutable branch parameter. For default push behavior, attached branch `foo` maps to remote branch URI `dml://<owner>/<project>#foo`.

Alternative considered:

- Allow mutable workflows from detached HEAD by implicitly using the last branch or default branch. Rejected because it reintroduces hidden branch-selection rules and makes history publication surprising.

### The change is breaking with no compatibility fallback

The implementation will reject old assumptions directly rather than silently translating them:

- no `DML_BRANCH`
- no `[branch].current`
- no local branch-qualified `project.uri`
- no dual-resolution path that checks old config before `.dml/HEAD`

Alternative considered:

- Add compatibility reads and rewrite-on-save behavior. Rejected because it permanently complicates config resolution, checkout semantics, and failure modes for a model the repository is intentionally replacing.

## Risks / Trade-offs

- Old repositories become invalid under the new rules -> Mitigation: state the break explicitly in proposal/spec/tasks and update tests/docs to fail closed rather than silently translating state.
- Detached commits can become dangling and easy to lose -> Mitigation: make detached semantics explicit in status/checkout responses and contract tests so the behavior is intentional and visible.
- Multiple command surfaces currently assume branch defaults from config -> Mitigation: centralize `HEAD` read/resolve behavior in `HeadOps` and resolver code so CLI/API/project workflows do not each invent fallback rules.
- Keeping `Dml(branch=...)` means API callers can still bypass checkout state deliberately -> Mitigation: document that this is an explicit API override, not repository truth, and keep default runtime behavior aligned with `.dml/HEAD`.

## Migration Plan

There is no backward-compatible migration plan.

- Repositories using the old branch-qualified local config model are out of contract once this change lands.
- `DML_BRANCH` is removed rather than deprecated.
- Any repository or automation that still depends on old config/env semantics must be rewritten manually outside product compatibility guarantees.
- Rollback, if needed during development, is code rollback before release rather than runtime dual-format support.

## Open Questions

- None for the core model. The repository truth boundary, detached semantics, `push` default, and no-compatibility stance are all decided.
