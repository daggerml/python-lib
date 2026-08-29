## Why

`DmlOps.init` still behaves like a directory scaffolder instead of a strict project initializer rooted at the current location. That causes ambiguous project placement and allows invalid or unresolved configuration (especially remote URI) to slip past initialization, which then fails later in pull/sync flows.

## What Changes

- Change `DmlOps.init` semantics to initialize only in the current location by creating `.dml/` locally, instead of creating a new project directory from a `here`/path-placement mode.
- Require `DmlOps.init` to accept standard config inputs and resolve them through the shared internal config resolver before proceeding.
- Enforce resolver-backed validation for required values (notably valid `remote.uri` when required by the init flow), and fail fast on invalid/unresolved configuration.
- Add recovery behavior for partially initialized repos: when `.dml/config.toml` exists but `.dml/db/` does not, create the DB and run pull when a project URI is configured.

## Capabilities

### New Capabilities
- `dmlops-init-recovery`: Define deterministic recovery behavior for `DmlOps.init` when config exists but local DB is missing.

### Modified Capabilities
- `shared-internal-configuration`: `DmlOps.init` must resolve and validate init-time config via the shared resolver, including required-field enforcement.
- `git-like-commit-ops`: Project init workflow semantics change to local `.dml/` initialization only, and remove directory-creation placement behavior.
- `required-remote-config`: Init and remote-aware setup paths enforce normalized, validated `remote.uri` from shared resolution rather than optional/late handling.

## Impact

- Affected code: `DmlOps` init orchestration and config handoff paths, plus related CLI/API call sites that supply init options.
- Affected behavior: project initialization location, validation timing, and bootstrap/recovery when local DB is absent.
- User-facing impact: clearer init contract (initialize "here" only), earlier actionable errors for bad config, and better auto-recovery for existing `.dml/config.toml` states.
