## Context

The current DML git-like surface mixes three sources of truth:

- current code behavior in `Dml`, `Head`, `CommitOps`, and remote ref handling,
- older specs that still mention named remotes, old detached-head behavior, and payload shapes that no longer match code,
- missing ref-lifecycle commands for branches and tags even though the rest of the repo model already treats refs as the main mutable user-facing handle.

The intended simplification is not "Git exactly." It is a DML-native ref model with Git-like operations:

- local mutable refs: branches,
- local immutable refs: tags, where mutation is delete-then-create,
- fetched remote refs addressed by canonical `dml://...` selectors,
- attached or detached `HEAD`,
- same-name tracking between local branches and fetched remote branches under the configured project.

This change is deliberately breaking. Backward compatibility is not a goal. Old command grammar, named-remote affordances, and belt-and-suspenders validation layers should be removed rather than preserved beside the new model.

## Goals / Non-Goals

**Goals:**

- Define one coherent command and ref model across code and specs.
- Expose the full lifecycle for local branches and local tags.
- Keep remote addressing explicit through `dml://...` syntax and same-name branch tracking.
- Add remote deletion through `push --delete <revision>` using the normal revision parser.
- Align detached commit, status, and show/diff semantics with actual DML behavior rather than legacy spec wording.
- Keep validation minimal: resolve and validate selectors only where the operation actually needs them, and rely on existing DB/ref validation instead of duplicating checks at every layer.

**Non-Goals:**

- Do not preserve named-remotes such as `origin`.
- Do not provide migration aliases, compatibility fallbacks, or parallel old/new command surfaces.
- Do not rebuild docs in this change; doc cleanup happens in the next sync.
- Do not introduce a more complex upstream-tracking configuration model than same-name branch tracking.

## Decisions

### Use Three Ref Families

The model should be:

- local mutable refs: branches,
- local immutable refs: tags,
- fetched remote refs addressed through canonical `dml://...` selectors.

This is simpler than Git's remote-name plus tracking-branch model and matches the DML repository shape more closely.

Alternative considered: separate published remote refs as a first-class user-facing family. Rejected because the user interacts with them through `dml://...` selectors and push/fetch workflows rather than through a local mutable namespace.

### Put Ref Lifecycle Under `branch` And `tag` Namespaces

The command surface should expose:

- `dml branch list`
- `dml branch create <name> [<revision>]`
- `dml branch move <name> <revision>`
- `dml branch rename <old> <new>`
- `dml branch delete <name>`
- `dml tag list`
- `dml tag create <name> [<revision>]`
- `dml tag delete <name>`

`move` is preferred over `reset` because it describes ref movement rather than a Git-specific working-tree concept.

Alternative considered: top-level branch commands or a single generic ref namespace. Rejected because separate branch/tag namespaces are clearer and match the agreed operational split.

### Keep Remote Syntax Explicit With `dml://...`

Remote selectors should use canonical DML URI syntax only. Named-remote shorthand such as `origin/main` should be removed from the maintained model. Local branch shorthand remains bare `main`; tag shorthand remains explicit `@tag` except inside the `tag` namespace, where the namespace itself provides the tag context.

Alternative considered: preserve `origin/...` as a convenience alias. Rejected because it conflicts with the explicit no-named-remotes direction and adds parser and docs complexity for a model the project no longer wants.

### Use `push --delete <revision>` For Remote Ref Deletion

Remote deletion should reuse the revision parser. Examples:

- `dml push --delete #foo`
- `dml push --delete @v1`
- `dml push --delete dml://acme/demo#foo`
- `dml push --delete dml://acme/demo@v1`

This keeps one selector model for create, update, and delete instead of inventing separate delete-only syntax.

Alternative considered: a separate `remote delete` command. Rejected because it duplicates selector handling and makes remote ref deletion feel like a different model from push/publish.

### Keep Same-Name Branch Tracking And Detached Remote Checkout

Fetched remote branches track local branches of the same name under the configured `remote.project`. `checkout dml://...#branch` remains detached. Users who want a local branch from that remote commit should run `branch create <name> dml://...#branch`.

Alternative considered: implicitly creating a local tracking branch on remote checkout. Rejected for now to keep `checkout` simple and avoid hidden ref mutations.

### Prefer Usage-Time Validation Only

The implementation should not add broad duplicate validation in orchestration code. Selector parsing and capability checks should happen when an operation actually needs them, and lower layers should continue relying on existing DB-backed and ref-path validation.

Alternative considered: repeated validation at command parsing, `Dml`, and lower-level ops boundaries. Rejected as belt-and-suspenders complexity that does not fit the project direction.

## Command Matrix

| Command / Surface | Current Code | Current Spec | Planned Action | Proposed Shape / Decision |
| --- | --- | --- | --- | --- |
| `dml branch list` | missing | implicit / partial | Create | list local branches |
| `dml branch create <name> [<revision>]` | missing | partial | Create / replace spec shape | create branch at resolved revision, default `HEAD` |
| `dml branch move <name> <revision>` | missing | missing | Create | repoint branch to resolved commit |
| `dml branch rename <old> <new>` | missing | missing | Create | rename branch ref; attached `HEAD` follows rename |
| `dml branch delete <name>` | missing | missing | Create | delete local branch |
| `dml branch --remote` | missing | present | Delete from spec | replace with fetched-remote ref handling elsewhere |
| `dml tag list` | missing | missing | Create | list local tags |
| `dml tag create <name> [<revision>]` | missing | missing | Create | create local tag at resolved revision, default `HEAD` |
| `dml tag delete <name>` | missing | missing | Create | delete local tag |
| `dml push` | exists | exists | Modify | push attached local branch to same-name remote branch in configured project |
| `dml push <revision>` | exists | exists | Modify | allow `#branch` and `@tag`; allow explicit `dml://...` only if it simplifies implementation |
| `dml push --delete <revision>` | missing | missing | Create | delete remote ref selected by normal revision parsing |
| `dml fetch <revision>` | exists | exists | Modify | `dml://...`, `#branch`, `@tag`; fetched refs are local cached remote refs |
| `dml pull` | exists | exists | Modify | attached branch only; fetch then merge same-name remote branch |
| `dml checkout <revision>` | exists | exists | Modify | local branch attaches; all other resolved revisions, including `dml://...`, detach |
| `dml checkout origin/main` | not supported | supported | Delete from spec | no named-remote shorthand |
| `dml dag checkout origin/main ...` | not supported | supported | Delete from spec | use `dml://...` only |
| `dml show origin/main` | not supported | supported | Delete from spec | use `dml://...` only |
| revision shorthand `origin/main` | not supported | supported | Delete from spec | remote refs addressed only by `dml://...` |
| local tag shorthand `v1.0` | not supported as bare name | supported | Modify spec | require `@v1.0` consistently outside tag namespace |
| `dml status` | exists | exists | Modify | report current head state, local branches, open indexes, ahead/behind; no DAG map in this change |
| `dml show` payload | exists | exists | Modify spec | use `diff.{added,removed,modified}` |
| commit while detached | code advances detached `HEAD` | spec says it does not | Modify spec | detached commit advances `HEAD` |

## Risks / Trade-offs

- [Breaking CLI/revision grammar] -> Mitigation: make the proposal explicit that backward compatibility is intentionally dropped and update all affected tests in one change.
- [Spec/code cleanup spans multiple capabilities] -> Mitigation: keep one authoritative matrix in this change and update specs together rather than piecemeal.
- [Remote branch UX may feel less Git-like] -> Mitigation: keep remote checkout detached but provide explicit `branch create <name> dml://...#branch` as the simple follow-up workflow.
- [Minimal validation could expose lower-level errors more directly] -> Mitigation: ensure operation boundaries report capability mismatches clearly while avoiding duplicated pre-validation.

## Migration Plan

1. Replace spec language that no longer matches code, especially detached commit, `status`, `show`, and remote shorthand.
2. Add the new branch and tag namespace methods and generated CLI commands.
3. Implement remote delete through `push --delete <revision>` and same-name tracking semantics.
4. Remove named-remote assumptions and old selector grammar from parsing and workflow tests.
5. Rewrite or update tests to the new command matrix and breaking behavior in one pass.

## Open Questions

- Should explicit `push dml://owner/project#branch` and `push dml://owner/project@tag` be supported immediately, or deferred unless they materially simplify implementation?
- Should local deletion of fetched remote refs be exposed as a first-class command now, or left out until a stronger need appears?
