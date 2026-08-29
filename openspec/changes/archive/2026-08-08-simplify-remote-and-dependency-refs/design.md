## Context

The current remote model addresses multiple projects inside one S3 root through `dml://` project URIs while `remote.root` separately identifies the storage and execution endpoint. Named ordinary remotes add another identity layer. In practice, one `remote.root` already serves one working project and its execution coordination, while reusable external work needs a narrower import-only capability.

The new model makes `remote.root` the project's only synchronization and execution endpoint. Named dependencies are separate local fetch configurations used only to inspect and import committed DAG work. No endpoint identity is persisted in DML objects: imported DAGs retain only immutable object refs.

## Goals / Non-Goals

**Goals:**

- Make `remote.root` the single project sync, CAS, cache, and execution endpoint.
- Remove project URIs and named ordinary remote identity from transport, configuration, refs, and revision grammar.
- Keep fetched remote refs separate from named dependency refs in local state.
- Prevent public APIs from directly pushing to, pulling from, merging from, rebasing onto, reverting, or setting upstreams to dependencies.
- Allow callers to fetch one dependency branch or tag, inspect it, load its DAGs, and import nodes into local DAGs.
- Preserve closure-based CAS publication so imported DAGs remain executable after cloning the destination project.
- Keep revision text about commit traversal and branch/tag names; select its ref namespace through explicit arguments.

**Non-Goals:**

- Enforce provenance after a dependency commit is addressed through a direct commit ref or local alias.
- Enforce authorization without backend credentials or IAM policy.
- Persist source endpoint, branch, tag, or dependency name in commits, trees, DAGs, or nodes.
- Implement shallow object closure fetch, worktrees, remote mirroring policy, or a generic backend plugin system.
- Migrate existing project-URI remote roots in place.
- Modify `daggerml._cli`; all CLI changes are generated from the shared public `Dml` surface.

## Decisions

### One project and execution domain per remote.root

`remote.root`, such as `s3://bucket/projects/models`, contains direct `refs/heads/*`, `refs/tags/*`, CAS, caches, and execution state. The `projects/<owner>/<project>` transport level, `ProjectUri`, `remote.project`, `remote.remotes`, and `origin` convention are removed.

Fetched refs from this endpoint are local tracking state under `.dml/refs/remote/heads/` and `.dml/refs/remote/tags/`. They contain commit pointers only; endpoint configuration remains canonical `remote.root` configuration.

`pull` and `push` always use resolved `remote.root`. `remote_root` remains a normal `Dml` construction/configuration input: it affects methods only when they perform remote-backed behavior, just as database configuration affects methods only when they use the database. No command-specific root override path is introduced.

### Named dependency state

Dependencies are stored under `.dml/refs/dep/<name>/`. Each directory contains `config.json`, `heads/`, and `tags/`. The initial config schema is exactly `{"backend":"s3","root":"s3://..."}`; unknown fields are rejected until a later schema version defines them.

`dml dep add NAME ROOT`, `dml dep list`, and `dml dep delete NAME` manage dependency endpoints. Dependency names are single validated path segments. Deleting a dependency removes its configuration and tracking refs, hence their local GC roots. Imported DAG closures remain live when referenced by local commits or runtimes.

### Fetch selects one endpoint and ref

`dml fetch [--dep DEP] [BRANCH|@TAG]` fetches from `remote.root` unless `--dep` names a dependency. With no selector it fetches branch `default.branch_name`. A branch selector fetches that branch; an `@tag` selector fetches that tag. Fetch validates and materializes the complete selected commit closure before replacing its tracking ref. Any fetch failure preserves the existing tracking ref; already-downloaded immutable objects may remain as unreferenced CAS data.

Fetch is the only operation here that contacts an endpoint for revision availability. Revision resolution against tracking refs remains local and reports that fetch is required when a selected ref is absent.

### Revision grammar is independent of source namespace

Revision strings retain existing local forms such as branch names, `@tag`, `HEAD`, ancestry, commit IDs, and exact commit refs. They never encode remote or dependency names.

Public revision-consuming APIs expose mutually exclusive source selectors: `remote=True` selects `.dml/refs/remote`, while `dep=NAME` selects `.dml/refs/dep/<name>`. Omitting both selects local refs. Every revision grammar form is accepted with every source selector. Resolution uses the selected namespace where the form requires symbolic state and succeeds whenever the requested commit is locally resolvable; otherwise it raises a descriptive error. Exact commit IDs and refs resolve from the local object database regardless of source selection. Frontends normalize source arguments once before revision resolution.

Inspection, `api.load`, and DAG checkout accept either selector. Repository checkout, branch/tag creation from a revision, merge, rebase, and revert expose `remote` but not `dep`. Direct commit refs and local aliases can still address dependency-derived commits; preventing that is explicitly not a provenance guarantee.

For `diff`, the source selector applies to the primary revision. An explicit `relative_to` remains local; when omitted, the comparison uses the selected commit's parent.

### Branch upstreams and synchronization

An attached local branch may track one branch name on `remote.root`; upstream state no longer stores a remote name. Pull fetches that branch from `remote.root` and merges its tracking ref. Push publishes to that branch. A first successful push of an untracked branch records the same branch name as its upstream; failed publication leaves it untracked.

### DAG consumption and publication

`api.load(name, dml=None, *, revision="HEAD", remote=False, dep=None)` resolves a commit tree from the selected source and returns its committed DAG. `Dag.require(dag: str | Dag, node_name=None, *, name=None)` accepts a properly loaded committed `Dag` in addition to its existing local-name form. Missing or incompatible DAG refs are rejected through normal API validation.

Revision-aware DAG checkout copies a selected DAG ref into the current tree. An import stores existing `ImportNode(dag, node)` refs only. When the destination project is pushed, normal closure traversal uploads every reachable dependency DAG, node, and data object to `remote.root`. Source commits, endpoints, and ref names are not retained unless independently reachable.

### Garbage collection

Local GC roots include local refs, remote tracking refs, dependency tracking refs, and existing runtime roots. Remote GC traces from direct published heads and tags. Removing a dependency removes only its tracking roots; imported objects remain reachable through local commits or runtimes.

## Risks / Trade-offs

- [Existing roots and DML project URIs stop working] -> Version the remote descriptor/layout and provide a breaking-change migration guide; do not silently read both layouts.
- [A command selects an unavailable revision] -> Accept the grammar form, never fetch implicitly, and raise a descriptive local-resolution error.
- [A dependency commit is aliased then merged] -> Treat dependency restrictions as public API capability boundaries, not persisted provenance enforcement.
- [User expects dependency provenance after publication] -> Document closure copying and support commit-message provenance; revisit persisted provenance only for a concrete use case.
- [Revision-source flags proliferate] -> Expose `dep` only on inspection and DAG-consumption APIs and `remote` only where remote tracking is meaningful; normalize both at one internal boundary.

## Migration Plan

1. Introduce a new remote descriptor and direct one-project ref layout. First publication conditionally initializes a truly empty root; missing descriptors on non-empty or legacy roots are rejected.
2. Remove project URI and named ordinary remote configuration and configure the project endpoint solely through `remote.root`.
3. Re-fetch the project endpoint into `.dml/refs/remote/` and register external sources with `dml dep add`.
4. Republish required branches and tags to one-project roots.
5. Remove URI parsing, owner/project ref paths, named remote APIs, and legacy compatibility in the same release.
6. Roll back by using the previous DML release and legacy remote root; the layouts are intentionally incompatible.
