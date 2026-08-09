## ADDED Requirements

### Requirement: Generated CLI exposes remote-root sync and dependency commands
The generated CLI SHALL expose `dep add|list|delete`, `fetch [--dep DEP] [BRANCH|@TAG]`, no-positional-argument `pull` and `push`, and revision-source flags solely from public `Dml` signatures. Merge, rebase, and revert SHALL expose `--remote` but not `--dep`. Methods exposing both source flags SHALL validate mutual exclusion at the shared `Dml` boundary. This change SHALL NOT modify `src/daggerml/_cli.py`.

#### Scenario: Fetch exposes optional dependency and ref
- **WHEN** a user views `dml fetch --help`
- **THEN** help shows optional `--dep DEP` and optional positional branch or `@tag`

#### Scenario: Dependency lifecycle is exposed
- **WHEN** a user views `dml dep --help`
- **THEN** help shows `add`, `list`, and `delete` without named ordinary remote commands

#### Scenario: Revision source flags are mutually exclusive
- **WHEN** a command exposes both `--remote` and `--dep DEP` and a user supplies both
- **THEN** generated dispatch reaches shared method validation, which rejects the invocation before revision lookup

#### Scenario: History mutation exposes remote only
- **WHEN** a user views merge, rebase, or revert help
- **THEN** help exposes `--remote` and does not expose `--dep`

#### Scenario: Pull and push reject positional endpoints
- **WHEN** a user supplies a positional endpoint to pull or push
- **THEN** generated parsing rejects the extra argument

#### Scenario: CLI module remains unchanged
- **WHEN** the revised public `Dml` signatures are implemented
- **THEN** generated command behavior changes without editing `src/daggerml/_cli.py`

## REMOVED Requirements

### Requirement: Generated CLI exposes named-remote synchronization commands
**Reason**: Named ordinary remotes are removed from the public surface.
**Migration**: Configure `remote.root`, use dependency lifecycle commands, and select fetched revision sources with flags.
