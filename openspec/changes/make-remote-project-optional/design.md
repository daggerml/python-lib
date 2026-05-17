## Context

Current repository bootstrap and local project-config helpers still treat `remote.project` as part of repository identity. That conflicts with the existing runtime model, where many mutation and execution paths only need `remote.root`, while project sync flows need both `remote.root` and `remote.project`.

The change crosses shared config resolution, local config persistence, init entrypoints, and project-sync guards. It also removes the older name-derived init path in favor of explicit optional remote configuration.

## Goals / Non-Goals

**Goals:**

- Allow valid local repos whose config contains `remote.root` but omits `remote.project`.
- Make `remote.root` the capability gate for remote-backed mutation and execution.
- Make `remote.project` the capability gate for project-addressed sync behavior.
- Simplify init so it accepts only optional `remote_project` and optional `remote_root`.
- Restrict init-time fetch/checkout to cases where `remote.project` is configured.

**Non-Goals:**

- Changing remote ref layout or remote protocol payloads.
- Making remote-backed mutation work without `remote.root`.
- Adding alternate ways to derive project identity during init.

## Decisions

### Treat local `remote.project` as optional publication metadata

Local repository validity will no longer require `remote.project`. Shared config resolution will continue validating branchless URI shape when the value is present, but local config loaders and helper accessors must tolerate absence.

Alternative considered: keep `remote.project` mandatory in local config and only loosen init. Rejected because it preserves the same invalid local state boundary and continues conflating repo existence with publication identity.

### Split capability checks by operation class

Operations that create or mutate remote-backed runtime state will require `remote.root`. Project-addressed sync operations such as push, pull, fetch, and init-time checkout/fetch will additionally require `remote.project`.

Alternative considered: continue relying on config-loader failures in sync paths. Rejected because it yields the wrong semantics and error boundary.

### Remove name-derived init identity

`Dml.init()` and CLI init will stop accepting `name`. Init will accept optional `remote_project` and optional `remote_root`, reject `remote_project` without `remote_root`, persist config when remote settings are provided, and skip fetch/checkout when `remote.project` is absent.

Alternative considered: keep `name` as shorthand for deriving `remote.project`. Rejected because it reintroduces implicit publication identity and user-resolution requirements that this change is removing.

### Keep recovery/bootstrap conditional on configured project identity

Recovery for missing local DB state remains valid without `remote.project`. If `remote.project` is configured, init may fetch or check out project state using the configured remote context. If not, recovery only restores local repository state.

## Risks / Trade-offs

- [Local helpers may still assume project identity exists] -> Mitigation: audit `DmlProjectConfig` consumers and add explicit `remote.project` capability checks in project-sync paths.
- [Error behavior may shift from load-time failures to operation-time failures] -> Mitigation: define targeted spec requirements and tests for missing `remote.root` vs missing `remote.project`.
- [Removing `name` from init is breaking for callers and docs] -> Mitigation: capture the break explicitly in proposal/specs and update CLI/API contracts together.

## Migration Plan

- Update specs and docs first so the capability model is explicit.
- Change init entrypoints and local config helpers to allow missing `remote.project`.
- Move project sync validation to explicit operation guards.
- Update tests from name-derived init expectations to optional remote-project expectations.

## Open Questions

None.
