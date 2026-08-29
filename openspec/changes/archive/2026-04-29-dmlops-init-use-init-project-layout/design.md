## Context

`DmlOps.init` currently performs several project-layout responsibilities inline: creating `.dml/`, writing `.dml/.gitignore`, and writing `.dml/config.toml` (when absent). A shared helper, `init_project_layout(project_dir, cfg)`, already exists in internal config code and performs this bootstrap work.

The current duplication increases the chance that layout or config-writing behavior diverges between call sites. This is especially risky in init recovery paths where config and db presence are intentionally checked and acted on separately.

## Goals / Non-Goals

**Goals:**
- Route `DmlOps.init` layout/bootstrap creation through `init_project_layout`.
- Preserve existing init contract: arguments, returned keys/values, and error behavior.
- Keep recovery behavior intact (including when pull is required).
- Remove now-unused private helper code in `DmlOps` once delegation is complete.

**Non-Goals:**
- Changing CLI/API option semantics for init.
- Altering project URI derivation rules or remote requirements.
- Changing db creation strategy beyond reusing existing helper paths.

## Decisions

### Delegate layout bootstrap to `init_project_layout`
- **Decision:** Use `init_project_layout(root, DmlProjectConfig(...))` in `DmlOps.init` for writing `.dml/.gitignore`, `.dml/config.toml`, and ensuring `.dml/db/` exists.
- **Rationale:** Centralizes initialization layout logic into one shared internal implementation.
- **Alternative considered:** Keep inline writes and only call helper for new init flows. Rejected because partial delegation preserves duplication and drift risk.

### Preserve config-exists and db-exists gating semantics
- **Decision:** Continue deriving `config_exists`, `db_exists`, and recovery mode before mutation so pull/no-pull behavior remains unchanged.
- **Rationale:** Existing behavior is covered by recovery specs and tests; this refactor should not alter functional outcomes.
- **Alternative considered:** Recompute existence checks after helper call. Rejected because that can blur recovery-state detection and change pull triggering.

### Remove duplicated private helpers when obsolete
- **Decision:** Remove helper methods in `DmlOps` that only support the previous inline layout-writing path after equivalent behavior is routed through shared config utilities.
- **Rationale:** Reduces maintenance surface and future inconsistency.
- **Alternative considered:** Keep old helpers as wrappers. Rejected because wrappers can hide dead paths and reintroduce duplicate behavior.

## Risks / Trade-offs

- [Helper invocation could write config when not intended] -> Mitigation: only invoke layout helper in the same branch where init currently creates missing config/db state.
- [Slightly tighter coupling from ops module to config module helper] -> Mitigation: coupling already exists through `DmlConfig`/`DmlProjectConfig`; this change reuses that boundary.
- [Behavior drift in `.gitignore` or config formatting] -> Mitigation: preserve helper output contract and verify with existing init workflow tests.

## Migration Plan

1. Refactor `DmlOps.init` to construct a `DmlProjectConfig` from resolved init config and invoke `init_project_layout` when bootstrap creation is needed.
2. Remove obsolete private helper methods and update imports.
3. Update/adjust internal tests that asserted inline behavior details.
4. Run init-focused test suites to confirm no contract changes.

Rollback strategy: revert the refactor commit to restore prior inline layout implementation.

## Open Questions

- None identified; scope is an internal refactor with contract preservation.
