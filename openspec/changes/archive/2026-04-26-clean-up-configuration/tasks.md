## 1. Define Shared Internal Configuration

- [x] 1.1 Refactor configuration code so `_internal` owns one canonical configuration model and one scope-aware resolver used by both API and CLI.
- [x] 1.2 Implement the canonical config parameters `project.home`, `remote.project`, `db.path`, `remote.uri`, `user`, `default_branch`, `hooks.post-init`, `hooks.post-clone`, and `config_home`, removing overlapping canonical params such as `branch`, `remote.root`, and named-remote config fields.
- [x] 1.3 Keep explicit args, environment variables, project-local config, and global config as sources that normalize through the shared precedence rules for `project/runtime` and `global` scopes.
- [x] 1.4 Normalize `remote.project` to always include a branch, reject tag-form project URIs, expose `project.branch` as a helper, and default `db.path` from `project.home/.dml/db/` when unset.
- [x] 1.5 Keep remote configuration normalization in shared internal code so remote-aware ops receive resolved `remote.uri` values rather than reading raw env or config files directly.

## 2. Update API And CLI Frontends

- [x] 2.1 Update `daggerml.api` to consume the shared internal configuration resolver instead of reconstructing config from frontend state.
- [x] 2.2 Update CLI entry points to consume the same shared internal configuration resolver for supported operations.
- [x] 2.3 Remove duplicated or ad hoc frontend-specific configuration translation paths that bypass shared internal config behavior, including old `DML_REPO` and `DML_BRANCH` assumptions.

## 3. Verify And Document Frontend Parity

- [x] 3.1 Update configuration tests to cover shared API/CLI resolution behavior, scope-aware precedence, project URI normalization, `project.branch` helper behavior, `db.path` dynamic defaulting, and remote-config handoff behavior.
- [x] 3.2 Update `docs/configuration.md` and related docs to describe `_internal` as the shared config boundary for API and CLI, document the canonical config table, and document intentional CLI gaps caused by serialization limits.
- [x] 3.3 Run the relevant test coverage for config, API, and CLI paths affected by the cleanup.
