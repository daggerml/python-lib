## Why

Configuration is currently split across runtime config, global config, and project config helpers that live in one module but follow different shapes, precedence rules, and environment-variable conventions. That makes the configuration surface hard to reason about, increases duplication between the API and CLI frontends, and leaves mismatches between the documented contract and the behavior the code actually exposes.

## What Changes

- Define one canonical internal configuration contract owned by `_internal` and used by both `api.py` and the CLI.
- Normalize explicit arguments, environment variables, project-local config, and global config through one shared resolver with `project/runtime` and `global` scopes.
- Reduce overlapping config names to a smaller canonical set: `project.home`, `remote.project`, `db.path`, `remote.uri`, `user`, `default_branch`, and `hooks.post-{init,clone}`.
- Make `remote.project` canonical for project identity and branch context by normalizing it to always include a branch, never a tag, with `project.branch` exposed as a helper rather than a standalone config parameter.
- Default `db.path` dynamically from `project.home/.dml/db/` so thin runtimes can operate by setting env vars directly.
- Clarify which config values are canonical, where derived values come from, and which helpers are responsible for validation and precedence.
- Update API and CLI call sites to use the same shared config resolution path instead of frontend-specific translation.
- Document that some API-backed behaviors remain unavailable in the CLI where object/function serialization prevents a practical CLI surface.

## Capabilities

### New Capabilities
- `shared-internal-configuration`: Define the canonical internal configuration model and the shared resolution path used by both API and CLI frontends.

### Modified Capabilities
- `required-remote-config`: Clarify how required remote configuration is normalized and handed to remote-aware components through the shared internal config path.

## Impact

- Affected code: `src/daggerml/_config.py` or successor internal config modules, config consumers in `src/daggerml/api.py`, CLI entry points under `src/daggerml/_cli/`, and related tests/docs.
- Affected APIs: configuration dataclasses/helpers, env-var resolution, URI parsing helpers, and shared API/CLI config loading behavior.
- Affected systems: frontend bootstrap, remote configuration handling, and configuration-focused tests/docs.
