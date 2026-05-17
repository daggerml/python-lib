## Context

DaggerML already models research history as immutable `Commit` objects, branch `Head` pointers, and `Tree.dags` mappings. Remote sync already stores immutable CAS objects and refs in S3, but branch publication is currently snapshot/tag-like rather than project/branch-oriented.

This change introduces a monorepo-style remote namespace with many owners, projects, and branches sharing one CAS. It also defines git-like operations that move branch heads safely and create explicit commits for merges, reverts, and DAG checkout.

## Goals / Non-Goals

**Goals:**

- Support remote project refs at `refs/projects/<owner>/<project>/{heads,tags}/`, with mutable heads and immutable tags.
- Support local remotes such as `origin` mapped to a project URI and storage root.
- Support fetching an explicit project URI into a local remote-tracking namespace so users can checkout DAGs from other projects.
- Store fetched remote branches/tags locally under canonical normalized DML URIs.
- Initialize local projects under a project directory with all DML-managed state inside `.dml/`.
- Support shell hooks for `init` and `clone` so users can run their normal project bootstrap commands.
- Make `clone`, `fetch`, `pull`, and `push` behave like git at the branch level.
- Require safe push updates using both ETag compare-and-swap and fast-forward-only ancestry unless `--force` is specified.
- Provide DAG-level checkout from any commit-ish into the current branch as a new commit.
- Keep conflict handling explicit and deterministic.

**Non-Goals:**

- Individual DAG push/pull/package distribution is out of scope.
- Persistent merge-conflict state is out of scope for the initial design.
- Automatic tracking of imported DAG versions is out of scope.
- Non-S3 remotes are out of scope.

## Decisions

### Remote namespace

Use this remote layout under the protocol root:

```text
refs/projects/<owner>/<project>/heads/<branch>.json
refs/projects/<owner>/<project>/tags/<tag>.json
refs/cache/<cache_key>.json
refs/dags/<dag_id>.json
cas/sha256/<aa>/<bb>/<oid>
```

Project refs are namespaced by owner and project, while CAS remains shared across the monorepo. This gives project isolation for discovery and permissions without losing object deduplication.

### Mutable branch heads

Branch heads under `refs/projects/<owner>/<project>/heads/` are mutable pointers to commit manifests. Tags under `refs/projects/<owner>/<project>/tags/` are immutable named refs; creating an existing tag path MUST fail.

Project branch and tag refs use the same remote ref payload schema as existing manifest refs: `kind`, `schema`, `target`, `created_at`, `targets`, and `meta`. Project refs MUST point to commit manifests, MUST include `targets`, and MUST satisfy the same manifest-target integrity checks before a ref is written.

Alternative considered: keep only immutable `tags/<branch>/<commit>.json`. That makes exact snapshots simple, but it does not provide a natural latest branch pointer for `fetch`, `pull`, or `push`.

### Global and local config

Each local repo stores DML-managed state under `<project-directory>/.dml/`. The directory contains `.dml/config.toml`, the local object database directory `.dml/db/`, and `.dml/.gitignore` with `*` so DML internals are not committed to the enclosing source repository.

Global DML config stores user defaults and bootstrap hooks. Its config directory resolves in this order:

```text
1. $DML_CONFIG_HOME, if set
2. $XDG_CONFIG_HOME/dml, if set
3. ~/.config/dml
```

The global config file is `<config-home>/config.toml`, for example `~/.config/dml/config.toml`:

```toml
[user]
name = "alice"

[defaults]
branch = "main"

[hooks]
post-init = ["uv init"]
post-clone = ["uv sync"]
```

Each local repo stores project identity and named remotes in `.dml/config.toml`, for example:

```toml
[project]
name = "my-project"
owner = "alice"
uri = "dml://alice/my-project"

[branch]
current = "main"

[remotes.origin]
uri = "dml://alice/my-project"
bucket = "example-bucket"
prefix = "team-monorepo"
```

Required project-local fields are:

| Field | Purpose |
| --- | --- |
| `[project].name` | Local project name. |
| `[project].owner` | Project owner. |
| `[project].uri` | Canonical project URI, `dml://<owner>/<project>`. |
| `[branch].current` | Current local branch name. |
| `[remotes.<name>].uri` | Remote project URI. |
| `[remotes.<name>].bucket` | Remote S3 bucket. |
| `[remotes.<name>].prefix` | Remote S3 prefix containing the DML protocol root. |

Configuration resolution uses waterfall precedence:

```text
explicit CLI/API argument > environment variable > config file value
```

Environment variables use `DML_` names for config values, including `DML_USER`, `DML_DEFAULT_BRANCH`, `DML_PROJECT_NAME`, `DML_PROJECT_OWNER`, `DML_REMOTE_PROJECT`, `DML_BRANCH`, `DML_REMOTE`, `DML_REMOTE_URI`, `DML_REMOTE_BUCKET`, and `DML_REMOTE_PREFIX`. Explicit command arguments always win over environment variables and config.

The supported DML environment variable surface for this project model is:

| Env var | Role |
| --- | --- |
| `DML_CONFIG_HOME` | Global DML config directory override. |
| `DML_USER` | User identity and default project owner. |
| `DML_BRANCH` | Selected branch override for commands. |
| `DML_DEFAULT_BRANCH` | Global default branch override for init/branch defaults. |
| `DML_PROJECT_NAME` | Project name override and hook context. |
| `DML_PROJECT_OWNER` | Project owner override and hook context. |
| `DML_REMOTE_PROJECT` | Canonical local remote project override. |
| `DML_PROJECT_HOME` | Hook context: absolute project root directory. |
| `DML_HOOK` | Hook context: hook name such as `post-init` or `post-clone`. |
| `DML_REMOTE` | Selected named remote override. |
| `DML_REMOTE_NAME` | Hook context: remote name such as `origin`. |
| `DML_REMOTE_URI` | Remote project URI override. |
| `DML_REMOTE_BUCKET` | Remote S3 bucket override. |
| `DML_REMOTE_PREFIX` | Remote S3 prefix override. |

The following legacy environment variables are removed from this project model and MUST NOT be used by new git-like project operations:

| Env var | Replacement |
| --- | --- |
| `DML_REPO` | `.dml/db/` under the resolved project directory. |
| `DML_REMOTE_ROOT` | `[remotes.<name>].bucket` and `[remotes.<name>].prefix`, or `DML_REMOTE_BUCKET` and `DML_REMOTE_PREFIX`. |
| `DML_DYNAMODB_TABLE` | None; DynamoDB execution state is out of scope. |
| `DML_REMOTE_CACHE` | None; legacy cache naming is out of scope. |

Project creation defaults owner to global `[user].name`, so creating `my-project` yields `dml://alice/my-project` unless an owner is explicitly provided. Branch creation defaults to global `[defaults].branch` when applicable, falling back to `main` when unset.

### Init and clone directory setup

`dml init <name>` creates `<name>/`, initializes `<name>/.dml/`, writes `.dml/config.toml`, writes `.dml/.gitignore` containing `*`, creates the local object database under `.dml/db/`, and creates an initial branch with an empty initial commit/tree.

If `<name>/` already exists, `dml init <name>` fails. Users who want to initialize an existing directory must `cd` into that directory and run `dml init --here <name>`.

`dml init --here <name>` initializes the current directory instead of creating a child directory. The project name still comes from `<name>`. Hooks still run for `--here` unless the user also specifies `--no-hooks`.

`dml clone dml://<owner>/<project>` creates a local project directory, initializes `.dml/` the same way as `init`, records `origin`, fetches the selected remote branch, and initializes the local branch state from it.

When clone is given only a project URI, it clones the configured default branch, falling back to `main`. A user may clone a different branch by specifying a branch commit-ish, for example `dml clone dml://alice/demo#experiment`.

### Init and clone shell hooks

`init` and `clone` support configured shell hooks for user-defined project bootstrap commands such as `uv init`. Hooks run in the project directory after it is created and after `.dml/` exists. Hook failures MUST stop the command and report the failing hook.

Bootstrap hooks are read from global DML config because project-local config does not exist until the command creates `.dml/config.toml`. Hook keys are ordered lists named `post-init` and `post-clone`:

```toml
[hooks]
post-init = ["uv init"]
post-clone = ["uv sync"]
```

Commands run hooks in listed order with the project directory as the working directory. `init` runs only `hooks.post-init`; `clone` runs only `hooks.post-clone`. `dml init --no-hooks <name>` and `dml init --here --no-hooks <name>` skip `post-init`; `dml clone --no-hooks <uri>` skips `post-clone`.

Hook commands receive environment variables describing the invocation:

```text
DML_HOOK=post-init|post-clone
DML_PROJECT_HOME=/absolute/path/to/project
DML_PROJECT_NAME=<project>
DML_PROJECT_OWNER=<owner>
DML_CONFIG_HOME=<resolved-global-config-home>
DML_BRANCH=<branch>
```

Clone hooks also receive:

```text
DML_REMOTE_NAME=origin
DML_REMOTE_URI=dml://<owner>/<project>
```

Hooks are intentionally shell commands rather than Python callbacks so users can reuse their normal project setup tools.

### Fetched remote tracking refs

Fetched remote branch and tag pointers are tracked locally by their canonical DML URI. The underlying DB representation is an implementation detail and is not part of user-facing command syntax.

Canonical tracking URIs are:

```text
dml://alice/tools#main
dml://alice/tools#feature/x
dml://alice/tools@v1.0
```

User-facing commands accept DML URI commit-ish values such as `dml://alice/tools#main` or configured remote shorthands such as `origin/main`; these resolve locally to the commit associated with the canonical DML URI.

DML URIs used for project refs MUST be canonicalized before storage. Canonical project tracking URIs include only owner, project, and a concrete branch or tag identifier. Derived expressions such as `HEAD~2` are never stored as tracking URIs; if a user fetches or resolves an expression, the stored tracking ref uses the canonical remote branch or tag URI that produced the fetched commit.

Canonical DML URIs MUST be no longer than the current ref-id limit of 64 bytes. Commands that create, configure, fetch, or push remote project refs MUST validate this limit before writing local heads or remote refs.

The 64-byte limit is accepted for the initial project model. Overlong ASCII URIs fail validation rather than being hashed or stored through an alternate compatibility layer.

Remote push/fetch MUST parse canonical DML URIs into structured remote paths such as `refs/projects/<owner>/<project>/heads/<branch>.json` or `refs/projects/<owner>/<project>/tags/<tag>.json`. Implementations MUST NOT use raw DML URI strings as remote object paths.

### Fetch, pull, push, clone

- `clone` creates a project directory, initializes `.dml/`, initializes local state from a remote project branch, and records `origin`.
- `fetch` downloads a remote branch head and materializes it into a local tracking ref for a canonical URI such as `dml://alice/my-project#main`.
- `fetch dml://<owner>/<project>[identifier]` downloads an explicitly addressed project branch or tag and materializes it into a local tracking ref so users can inspect or checkout DAGs from that project without merging it into their current project.
- `pull` performs `fetch` and then merges the fetched remote-tracking head into the current/local branch.
- `push` uploads missing CAS/manifests and updates an existing remote branch head. If the remote branch does not exist, push fails unless `--create` is provided.

Push MUST be fast-forward-only unless `--force` is provided. Push MUST still use an ETag conditional update even with `--force`, so force bypasses ancestry safety but not concurrent-update safety. `--create` writes only when the remote branch ref does not already exist; if another client creates the branch first, the create push fails.

### Merge conflicts

Merge operates on `Tree.dags`. A conflict occurs when both sides changed the same DAG name differently since the merge base. Initial UX aborts with a structured conflict list. Strategy flags can be added without introducing persistent conflict state.

### DAG checkout

Use one command for DAG-level extraction from history:

```bash
dml dag checkout <commit-ish> <source-name> [--as <target-name>] [--replace]
```

The command resolves `<commit-ish>`, reads `<source-name>` from that commit's tree, writes the DAG ref into the current branch tree under the target name, creates a new commit, and advances the current head. Existing target names require `--replace` unless the ref is unchanged. Because explicit URI fetches create local remote-tracking heads, users can fetch another project and then checkout one of its DAGs into the current project.

This covers DAG revert use cases without a separate `dml dag revert`; users can checkout from `HEAD~N`, a branch, a remote-tracking branch, a fetched DML URI, or an explicit commit ref. Commit-ish resolution is local-only: DML URIs resolve through existing local tracking refs and MUST NOT implicitly fetch from the network.

## Risks / Trade-offs

- Concurrent remote updates can race -> use ETag compare-and-swap for branch head writes.
- Fast-forward checks require commit ancestry availability -> fetch/materialize the remote head before evaluating push safety.
- Abort-only merge conflicts may be less convenient than git's conflict index -> start simpler and add explicit resolution flags later if needed.
- `commit-ish` parsing can become complex -> implement a small, documented local-only grammar first: full commit refs, local branch shorthands, configured remote shorthands, fetched DML URI tracking refs, `HEAD`, and `~N` ancestry. Keep internal DB refs out of user-facing syntax.
- Mutable branch refs require schema/version care -> keep branch head ref payloads validated and include target manifest metadata.
