## Why

Remote configuration currently separates the transport root from project URI identity and named ordinary remotes even though one configured remote root serves both project synchronization and execution coordination. That duplicates identity, complicates revision grammar, and lets dependency sources look like supported merge or push targets.

## What Changes

- **BREAKING** Remove DML project URIs, `ProjectUri`, owner/project transport paths, `remote.project`, and named ordinary remotes.
- **BREAKING** Make `remote.root` the sole project synchronization, CAS, cache, and execution-coordination endpoint.
- **BREAKING** Store remote refs directly at `refs/heads/*` and `refs/tags/*`, with fetched tracking refs under `.dml/refs/remote/{heads,tags}/`.
- Add named import dependencies through `dml dep add|list|delete`, stored under `.dml/refs/dep/<name>/` with endpoint config and fetched refs.
- Change fetch to `dml fetch [--dep DEP] [BRANCH|@TAG]`; it fetches from `remote.root` by default and uses `default.branch_name` when no selector is supplied.
- Keep revision strings namespace-independent. Public APIs select fetched remote or dependency refs through mutually exclusive `remote` and `dep` arguments.
- Keep `daggerml._cli` unchanged; its command surface continues to be generated from public `Dml` signatures.
- Allow remote revision selection for history workflows; allow dependency selection only for inspection, DAG loading, and DAG checkout/import workflows.
- Add a revision argument to `api.load()` and support importing a loaded committed DAG into a new DAG.
- Preserve self-contained publication by transferring every object reachable from imported DAG refs into `remote.root` CAS.

## Capabilities

### New Capabilities
- `dependency-dag-imports`: Configure, fetch, inspect, and consume external project DAGs through import-only dependency refs.

### Modified Capabilities
- `admin-cli-controls`: List refs directly from `remote.root` without project discovery or project URIs.
- `clone-bootstrap-workflow`: Clone from a remote root and persist it as `remote.root`.
- `dml-resolution`: Resolve namespace-independent revisions against an explicitly selected local tracking namespace.
- `dmlops-init-recovery`: Recover remote-backed state based on `remote.root` rather than project identity.
- `generated-dml-cli`: Replace named-remote commands with dependency lifecycle and revision-source flags.
- `git-like-commit-ops`: Track one remote root, use branch-only upstreams, and select remote revisions by flag.
- `init-input-normalization`: Remove project identity from initialization.
- `named-remote-branch-tracking`: Remove named ordinary remotes and retain branch-only upstream tracking.
- `remote-project-refs`: Replace project-addressed refs and configuration with one-project-per-root transport and tracking.
- `revision-parsing-contract-matrix`: Separate revision grammar from local, remote, and dependency source selection.
- `required-remote-config`: Use `remote.root` directly for synchronization and execution behavior.
- `shared-internal-configuration`: Remove `remote.project` and project URI normalization.
- `test-contract-matrix`: Remove project URI generation from parsing tests.
- `thin-cli-routing`: Route the revised fetch and source-selection command shapes through `Dml`.
- `unified-dml-surface`: Describe revision-source arguments without project URI forms.

## Impact

This changes `src/daggerml/_core/remote.py`, `head.py`, `config.py`, `dml.py`, `revision.py`, `uri.py`, and `api.py`, plus CLI output generated from the revised `Dml` surface. It MUST NOT change `src/daggerml/_cli.py`. It removes the DML project URI model and named ordinary remote APIs, changes remote wire and local tracking layouts, updates configuration and revision APIs, replaces remote tests, and requires updates to history, sharing, configuration, CLI, authoring, and architecture documentation.
