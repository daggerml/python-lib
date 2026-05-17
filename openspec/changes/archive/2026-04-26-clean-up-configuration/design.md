## Context

Configuration concerns are currently concentrated in `src/daggerml/_config.py`, which mixes runtime resolution, global config loading, project config persistence, project-layout helpers, and hook execution. At the same time, `api.py` and the CLI both sit on top of `_internal`, but they do not yet read as thin bindings over one shared configuration model.

The cleanup needs to make `_internal` the clear package boundary for configuration and operations without weakening existing contracts such as project-local `.dml/config.toml` and explicit required remote configuration for remote-backed flows. It also needs to document that frontend parity is the goal for shared config/ops behavior even though some behaviors remain API-only because CLI serialization cannot represent them cleanly.

## Goals / Non-Goals

**Goals:**
- Define one canonical internal config model and one shared resolution path used by both API and CLI.
- Keep one resolver implementation while supporting two scopes: `project/runtime` and `global`.
- Preserve the distinction between different config sources while making them inputs to the same resolved internal config.
- Reduce the canonical config surface to a small set of URI- and path-based fields with helper accessors for parsed components.
- Make derived values, especially remote configuration handed to remote-aware ops, flow from resolved config objects rather than ad hoc env/file reads in frontends.
- Update docs and tests so configuration behavior is described consistently across API and CLI frontends.
- Explicitly document which functionality is intentionally missing from the CLI because serialization constraints prevent a practical command-line surface.

**Non-Goals:**
- Redesigning remote project refs, hook semantics, or storage layout.
- Adding new end-user configuration features beyond the cleanup needed to unify existing behavior.
- Introducing compatibility aliases for old or experimental env-var names that are outside the current documented contract.
- Eliminating API-only functionality that depends on Python object or function serialization the CLI cannot represent.

## Decisions

### Use one canonical internal config model with multiple source adapters
The implementation will define one canonical resolved config model owned by `_internal`. Explicit arguments, environment variables, project-local config, and global config remain distinct sources, but they must all feed the same internal resolution path consumed by both API and CLI frontends.

Alternative considered: keep separate runtime and project-command resolved models as the long-term boundary.
Why not: it conflicts with the intended architecture where `_internal` is the package and `api.py` and CLI are frontends over the same underlying behavior.

### Keep source-specific loading but centralize normalization in `_internal`
Project-local config, global config, and environment-variable handling can still be loaded through source-specific helpers, but normalization, precedence, validation, and derivation belong in `_internal` shared code. Frontends should not embed their own config semantics.

Alternative considered: let each frontend adapt raw sources differently and only converge at ops calls.
Why not: it preserves duplicated behavior and makes API/CLI parity hard to test.

### Normalize remote configuration before remote-aware components are constructed
Remote-aware components should receive already-resolved `remote.uri` values from the shared internal resolver rather than inspecting raw environment variables or project config files directly. The cleanup removes overlapping remote config forms and keeps parsed remote bucket/prefix details as helpers on the resolved config object instead of separate canonical parameters.

Alternative considered: keep `remote.root`, `remote.uri`, and bucket/prefix config side by side.
Why not: multiple canonical representations for the same remote location are a major source of current config complexity.

### Normalize project identity into `remote.project`
The resolved config model will use `remote.project` as the canonical project identity, and the resolver will normalize that URI to always include a branch, defaulting from `default_branch` when needed. `remote.project` will never normalize to a tag form because tags are immutable and are not valid active project context. Code that needs the branch will use a `project.branch` helper on the resolved config object instead of a standalone canonical `branch` parameter.

Alternative considered: keep a separate canonical `branch` config parameter.
Why not: it duplicates information already carried by the normalized project URI and creates another overlap point between API and CLI.

### Keep `db.path` as an explicit overridable field with a dynamic default
The resolved config model will include `db.path`, with the same resolution order as other `project/runtime` fields, but its default will be computed dynamically as `project.home/.dml/db/`. This keeps thin runtimes workable because they can set `DML_DB_PATH` directly without requiring richer project config.

Alternative considered: derive the DB path only implicitly from `project.home` and never expose it as config.
Why not: some thin runtimes need to point directly at a DB path without carrying the rest of the project setup.

### Treat API and CLI as parity frontends with documented CLI gaps
The implementation should aim for API and CLI to use the same config and ops machinery under the hood. Where CLI cannot expose a feature because command-line serialization cannot faithfully represent Python-level inputs or outputs, the limitation should be documented explicitly rather than modeled as a config difference.

Alternative considered: make CLI omissions implicit and leave parity undefined.
Why not: it hides real product constraints and makes it unclear whether a behavior difference is intentional or a bug.

## Risks / Trade-offs

- [Cross-cutting caller updates] -> Update API, CLI, and shared internal config helpers in the same change and verify them together.
- [Behavior drift between docs and code] -> Treat the OpenSpec artifacts and `docs/configuration.md` as part of the same cleanup so the authoritative contract stays aligned.
- [Frontend parity assumptions become too strong] -> Document serialization-driven CLI gaps explicitly so parity expectations are accurate.
- [URI normalization edge cases] -> Add tests for project URIs with and without explicit branches and for canonical remote URI parsing helpers.

## Migration Plan

No persisted data migration is required. The change should land as one coordinated refactor that updates shared internal configuration helpers, their frontend callers, and documentation/tests together.

## Open Questions

- Whether the cleanup is best expressed as one reorganized `_config.py` module or as a small split into dedicated internal configuration modules can be left to implementation, as long as the scoped resolver and canonical config contract in the specs remain the same.
