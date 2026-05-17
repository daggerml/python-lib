## Context

`DmlOps.init` currently blends project placement, config setup, and repo bootstrap in ways that make behavior hard to reason about across CLI and API callers. The change introduces three coupled concerns: (1) init location semantics become fixed to the current working project root (`.dml/` here), (2) init-time options must be normalized and validated through the shared internal resolver before side effects, and (3) an existing-config/missing-db edge case must be recoverable without manual repair.

This is cross-cutting because the behavior touches orchestration in internal ops, caller input contracts, and remote bootstrap behavior (`pull`) that depends on correctly resolved project and remote settings.

## Goals / Non-Goals

**Goals:**
- Make `DmlOps.init` deterministic: initialize local project metadata under `.dml/` at the current location only.
- Ensure init inputs use canonical config resolution/validation (`explicit > env > project > global > defaults`) through shared internal config code.
- Fail fast when required config values cannot be resolved to valid values (especially `remote.uri` where required by downstream behavior).
- Support idempotent recovery when `.dml/config.toml` exists but `.dml/db/` is absent by creating DB and syncing project state when `remote.project` exists.

**Non-Goals:**
- Redesigning full clone/fetch/checkout workflow semantics outside of init bootstrap.
- Introducing new config keys or a new precedence model.
- Changing remote protocol, CAS layout, or merge semantics.

## Decisions

- Use local-root-only init semantics.
  - Decision: remove directory-creation placement from `DmlOps.init`; it creates `.dml/` in the current project location.
  - Rationale: aligns with git-like repository initialization semantics and removes ambiguity around `here`/path interpretation.
  - Alternative considered: keep `here` and deprecate later. Rejected because dual behavior would preserve ambiguity and complicate validation and caller contracts.

- Resolve and validate init config before mutating filesystem state.
  - Decision: run `DmlOps.init` options through the shared internal resolver and require canonical resolved fields before writing config or creating DB.
  - Rationale: guarantees consistent API/CLI behavior and shifts config failures to the earliest possible point.
  - Alternative considered: allow partial init then validate during pull. Rejected because it produces half-initialized states and deferred runtime failures.

- Treat existing config + missing DB as a supported recovery path.
  - Decision: if `.dml/config.toml` exists and `.dml/db/` is absent, init creates DB, then conditionally runs pull when resolved `remote.project` is present.
  - Rationale: this state appears after interrupted setup or manual migration; deterministic recovery avoids requiring users to hand-edit local metadata.
  - Alternative considered: fail and require manual remediation. Rejected because it increases operator burden and creates avoidable support complexity.

- Require normalized remote configuration for remote-aware bootstrap.
  - Decision: remote-aware init/pull path consumes validated `remote.uri` from shared resolver; invalid or unresolved required values fail init.
  - Rationale: enforces `required-remote-config` consistency and avoids hidden env/config probing in downstream components.
  - Alternative considered: allow remote URI omission and best-effort pull. Rejected because behavior becomes nondeterministic and failure modes become late.

## Risks / Trade-offs

- [Breaking caller behavior for placement options] -> Mitigation: update CLI/API validation and help text to remove `here`/directory-creation mode and document local-root-only semantics.
- [Stricter validation may fail previously tolerated setups] -> Mitigation: provide clear, field-specific errors from resolver-backed validation and preserve precedence rules users already depend on.
- [Auto-pull during recovery may surface remote errors during init] -> Mitigation: make pull conditional on `remote.project` presence and keep failure messages explicit about remote config or connectivity causes.
- [Idempotency regressions in repeated init calls] -> Mitigation: add tests for repeated init on clean, already-initialized, and partial states to verify stable outcomes.

## Migration Plan

- Update `DmlOps.init` call contract and implementation first, then align CLI/API entrypoints with the new argument/validation expectations.
- Add/adjust tests for local-root-only init, config validation failures, and recovery flow (`config exists`, `db missing`).
- Rollout is source-compatible for callers that already initialize in-place; callers relying on directory placement must switch to invoking init from target directory.
- Rollback strategy: revert init contract changes and associated spec deltas in one patch if critical compatibility issues arise.

## Open Questions

- Should recovery-mode pull be best-effort with warnings or fail-hard when `remote.project` is present but remote configuration is invalid? Current direction is fail-hard for consistency with strict init validation.
