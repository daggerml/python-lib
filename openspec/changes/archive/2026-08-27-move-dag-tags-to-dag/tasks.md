## 1. Persist Intrinsic DAG Tags

- [x] 1.1 Add normalized required tags to the core DAG model, remove tree tag storage and validation, and verify core type and serialization contract tests cover sorted, unique string tags and the intentional format break.
- [x] 1.2 Update index creation, mutation, commit, error-result, and DAG inspection paths to retain intrinsic tags atomically with the DAG ref, and verify committed and execution-published DAGs retain tags.
- [x] 1.3 Simplify commit/tree operations, inspection payloads, merge/rebase/revert/checkout behavior, and remove tree-tag APIs; verify the former tree-tag contract suite is replaced with intrinsic-DAG behavior coverage.

## 2. Expose Runtime And Authoring Tags

- [x] 2.1 Add active-index `Dml.runtime.add_tag` and `remove_tag` operations with lifecycle mutation checks, normalization, idempotency, and frozen-index rejection; verify runtime contract tests cover each outcome.
- [x] 2.2 Make `dml.new(..., tags=...)` initialize core DAG tags and make live and loaded `api.Dag.tags` read the underlying DAG tags; verify public API contract and live-runtime integration tests.
- [x] 2.3 Remove the `tags` argument from `dml.resume`, preserve tags through freeze/unfreeze, and update generated API/CLI contracts; verify resume and freezing contract tests cover preservation and the removed signature.

## 3. Propagate Funk Tags

- [x] 3.1 Extend script funkification and script runnable lowering to accept, validate, normalize, and retain declared tags; verify script-executor resolver contracts reject invalid metadata and preserve normalized tags.
- [x] 3.2 Carry script runnable tags through the worker launch interface and create the worker DAG with those tags before source execution; verify successful and failing worker executions publish tagged DAGs.
- [x] 3.3 Verify tagged script funks under nested executor transports and cache reuse return result DAGs with their declared tags, using contrib integration coverage.

## 4. Document And Validate The Breaking Change

- [x] 4.1 Update Python authoring, DAG storage, funks/cache, history, and extension documentation to describe intrinsic DAG tags, active-runtime mutation, funk tags, and removed tree-tag/resume APIs; verify documentation links and examples are accurate.
- [x] 4.2 Update examples and all affected tag, commit, CLI, API, core, and contrib tests; run the focused suites and `pytest` to verify the repository-wide behavior.
- [x] 4.3 Run `openspec validate move-dag-tags-to-dag --strict` and verify all change artifacts are valid before implementation review.
