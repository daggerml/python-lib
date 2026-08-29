## 1. Remote Root and Dependency State

- [x] 1.1 Remove `ProjectUri`, `remote.project`, `remote.remotes`, named ordinary remote APIs, owner/project ref paths, and all `dml://` compatibility from configuration and revision operations.
- [x] 1.2 Refactor `Head` paths, listing, deletion, and GC-root enumeration for `.dml/refs/remote/{heads,tags}` and `.dml/refs/dep/<name>/{config.json,heads,tags}`; never parse dependency config files as commit refs.
- [x] 1.3 Implement strict v1 dependency endpoint configs containing only `backend` and `root`, plus `dml dep add|list|delete` lifecycle behavior.
- [x] 1.4 Define and validate the one-project-per-root remote descriptor and direct `refs/heads/*` and `refs/tags/*` transport layout, including conditional first-push initialization of an empty root and rejection of non-empty undescribed roots.

## 2. Sync and Capability Boundaries

- [x] 2.1 Refactor remote publication, selected-ref retrieval, conditional updates, and GC to operate directly against resolved `remote.root`.
- [x] 2.2 Implement `dml fetch [--dep DEP] [BRANCH|@TAG]`, defaulting to branch `default.branch_name`, updating tracking only after complete closure validation, and preserving the prior tracking ref on every failure.
- [x] 2.3 Store branch upstreams as remote branch names without endpoint names; update branch rename/delete, pull, push, status, and first-push behavior accordingly.
- [x] 2.4 Restrict pull and push to resolved `remote.root`; keep `remote_root` as ordinary `Dml` construction/configuration input rather than adding command-specific endpoint plumbing.
- [x] 2.5 Reject dependency source selection at repository checkout, upstream, branch/tag creation, merge, rebase, revert, pull, and push public boundaries.
- [x] 2.6 Update `Dml.clone(revision=None, /, *, project_home=".", remote_root=None, ...)` to resolve and persist `remote.root`, materialize the optional revision or default branch, and set attached or detached HEAD appropriately.

## 3. Revision and DAG Consumption

- [x] 3.1 Remove URI and namespace prefixes from revision grammar; normalize mutually exclusive `remote` and `dep` API arguments into one internal revision-source value.
- [x] 3.2 Accept every revision form with local, remote, or dependency source selection; resolve exact commits from the DB and symbolic forms from the selected namespace, raising without network access when resolution is impossible.
- [x] 3.3 Implement the specified public signatures for show, log, diff, rev-parse, repository/DAG checkout, merge, rebase, revert, branch creation, and tag creation with their permitted source arguments and consistent payload metadata.
- [x] 3.4 Define diff source behavior: source flags apply to the primary revision, explicit `relative_to` is local, and omitted `relative_to` uses the selected commit parent.
- [x] 3.5 Add `revision="HEAD"`, `remote=False`, and `dep=None` support to `api.load()` and resolve named DAGs from the selected commit tree.
- [x] 3.6 Extend `Dag.require()` to import a node from a properly loaded committed `Dag` while preserving the current local-name form and normal invalid-ref rejection.
- [x] 3.7 Add revision-aware DAG checkout with remote and dependency source selection.
- [x] 3.8 Verify closure publication and local/remote GC preserve imported DAG objects without persisting dependency identity.

## 4. Tests and Documentation

- [x] 4.1 Replace project-URI and named-remote tests with direct root transport, descriptor, ref-path, conditional publication, and remote-root configuration contracts.
- [x] 4.2 Add dependency lifecycle, selected branch/tag fetch, deletion, tracking isolation, and GC-root contract tests.
- [x] 4.3 Extend the centralized revision matrix with namespace-independent grammar, explicit source selection, mutual-exclusion errors, missing fetched refs, and operation capability cases.
- [x] 4.4 Add API and integration tests for revision-aware `api.load`, all revision/source combinations, dependency DAG imports, DAG checkout, self-contained pushed closures, fetch failure atomicity, and rejected dependency operations.
- [x] 4.5 Update CLI, configuration, history/remotes, sharing/reuse, authoring, architecture, glossary, errors, and migration documentation; remove all project URI and named ordinary remote guidance.
- [x] 4.6 Update `DOC_MAP.md` if dependency endpoint configuration requires a new topic/path mapping.
- [x] 4.7 Verify `src/daggerml/_cli.py` is unchanged; CLI behavior must derive entirely from public `Dml` signatures.
- [x] 4.8 Run targeted contract and integration suites, then `uv run --dev --all-extras pytest .` and `uv run --dev --all-extras ruff check --fix .`.
