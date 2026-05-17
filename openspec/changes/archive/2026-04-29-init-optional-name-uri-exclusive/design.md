## Context

`DmlOps.init` currently receives identity inputs that can overlap (`name`, `remote_project`) and existing behavior relies on implicit assumptions about how project URI is chosen. The requested change introduces an explicit identity selection contract: either the caller provides `remote_project`, or the caller provides `name` and the system derives URI ownership from resolved global config user. The implementation must preserve existing successful URI-based init flows while improving validation and user-facing diagnostics for unresolved user identity.

## Goals / Non-Goals

**Goals:**
- Enforce mutual exclusivity of `name` and `remote_project` in init input validation.
- Allow `name` to be omitted when explicit `remote_project` is provided.
- Derive canonical project URI from `name` and resolved global config user when `name` is provided.
- Produce descriptive, deterministic errors when user resolution is required but unavailable.

**Non-Goals:**
- Changing project URI schema or owner normalization rules beyond current contracts.
- Altering remote URI defaults or non-identity init configuration behavior.
- Expanding init to support additional identity sources.

## Decisions

- **Single identity source gate in `DmlOps.init`**: Centralize validation so exactly one of (`name`, `remote_project`) is used for project identity. This avoids duplicated logic between CLI and internal call paths.
  - Alternative considered: enforce only at CLI parsing level; rejected because programmatic callers of `DmlOps.init` would bypass constraints.

- **Derive URI only from `name` + resolved user**: If `name` is present, compute project URI using current owner derivation from global config user, and fail if user is unresolved.
  - Alternative considered: fallback to anonymous/default owner when user missing; rejected because it silently mutates identity semantics and can produce surprising project ownership.

- **Preserve explicit URI authority**: If `remote_project` is provided, treat it as authoritative and do not derive from `name`.
  - Alternative considered: allow both with precedence rules; rejected because precedence masks user mistakes and weakens contract clarity.

- **Improve error phrasing for unresolved user path**: Raise explicit repository/config errors that explain why initialization cannot proceed and what input mode avoids the requirement.
  - Alternative considered: generic "invalid init arguments" error; rejected as insufficiently actionable.

## Risks / Trade-offs

- **[Risk] Backward compatibility for callers passing both fields** → Mitigation: fail fast with explicit mutual-exclusion message so migration is straightforward.
- **[Risk] Existing tests may encode old required-name assumptions** → Mitigation: update/init tests to cover URI-only path, name-derived path, and unresolved-user failure path.
- **[Trade-off] Stricter validation may surface misconfigurations earlier** → Mitigation: provide actionable error text including required mode (`--name` vs `--remote-project`) and missing user guidance.

## Migration Plan

- Update init argument validation and URI derivation path in `DmlOps.init`.
- Align CLI-facing argument docs/help text with new contract.
- Add or update tests for:
  - URI-only initialization with omitted name.
  - Name-only initialization deriving URI from resolved user.
  - Rejection of simultaneous `name` and `remote_project`.
  - Rejection when `name` requires user resolution but user is unresolved.
- No data migration needed; change affects init-time validation/derivation only.

## Open Questions

- None; requested contract is explicit and can be implemented directly.
