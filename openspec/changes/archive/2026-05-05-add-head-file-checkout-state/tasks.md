## 1. Repository state model

- [x] 1.1 Extend `HeadOps` with public `.dml/HEAD` read/write/resolve methods for attached and detached payloads.
- [x] 1.2 Update repository bootstrap and init flows to create `.dml/HEAD` attached to the initial local branch.
- [x] 1.3 Add contract and integration coverage for valid attached/detached HEAD payloads and invalid HEAD failure modes.

## 2. Configuration and project layout

- [x] 2.1 Remove `DML_BRANCH` and any branch-selection config normalization from shared internal configuration resolution.
- [x] 2.2 Change local `DmlProjectConfig` persistence and validation so `remote.project` is branchless and local config no longer stores branch state.
- [x] 2.3 Update init, hook environment, and status/config surfaces to reflect branchless project config and the removal of `DML_BRANCH`.

## 3. Revision and checkout behavior

- [x] 3.1 Refactor revision resolution so `HEAD` and `HEAD~n` resolve through `.dml/HEAD` instead of a caller-supplied current branch.
- [x] 3.2 Update repository checkout workflows to rewrite `.dml/HEAD` for attached and detached modes.
- [x] 3.3 Preserve immutable detached semantics so detached commits do not advance any branch ref or rewrite `.dml/HEAD`.

## 4. Mutable workflow gating

- [x] 4.1 Update project push defaults so attached local branch `foo` publishes to `dml://<owner>/<project>#foo`.
- [x] 4.2 Require attached HEAD or an explicit mutable branch target for project pull, merge, revert, and similar branch-mutating workflows.
- [x] 4.3 Keep the Python API `Dml(branch=...)` override available while making default runtime behavior derive checkout state from `.dml/HEAD`.

## 5. Documentation and verification

- [x] 5.1 Rewrite affected OpenSpec-backed tests for config precedence, init, revision parsing, checkout, and push/pull behavior under the new model.
- [x] 5.2 Update repository docs to state the breaking change explicitly, including the lack of backward compatibility for old local config and `DML_BRANCH`.
- [x] 5.3 Run the relevant test suites covering config, head ops, revision parsing, project workflows, and API defaults.
