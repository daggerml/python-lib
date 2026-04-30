## Context

`clone` behavior currently spans CLI routing, `DmlOps` orchestration, and remote/project bootstrap rules. This creates duplicate bootstrap paths alongside `init`, plus clone-only hook/config branches that increase complexity and weaken the thin-wrapper CLI boundary. The change removes clone end-to-end and keeps `init` as the only repository bootstrap flow, while preserving existing non-clone git-like operations (`fetch`, `checkout`, `pull`, `push`, `merge`, `revert`).

Constraints:
- No backward compatibility for clone behavior or aliases.
- CLI must remain a thin adapter over public `daggerml._internal` APIs.
- Init behavior and recovery guarantees must remain intact.

## Goals / Non-Goals

**Goals:**
- Remove all user-facing and internal clone entrypoints and data paths.
- Keep project lifecycle coherent around `init` plus explicit remote synchronization commands.
- Eliminate dead code and clone-only test/doc surface.
- Tighten CLI architecture so handlers only parse arguments and call one internal API path.

**Non-Goals:**
- Replacing clone with a new single-command bootstrap workflow in this change.
- Adding migration shims, deprecation windows, or compatibility aliases.
- Redesigning non-clone remote protocol semantics.

## Decisions

1. Remove clone at the contract layer first (OpenSpec deltas), then enforce in implementation.
   - Rationale: specs become the source of truth for deleting behavior and guide required code/test/doc removals.
   - Alternative considered: removing code first and backfilling specs; rejected because it risks partial behavior drift.

2. Preserve `init` as the sole bootstrap primitive and require explicit remote actions after init.
   - Rationale: simpler mental model and clearer failure boundaries; avoids hidden fetch/checkout side effects.
   - Alternative considered: folding clone semantics into init flags; rejected to keep scope focused and avoid reintroducing implicit bootstrap orchestration.

3. Keep CLI command modules as thin wrappers over internal APIs and forbid CLI-owned orchestration.
   - Rationale: consolidates business logic in internal APIs, reducing duplication and improving testability.
   - Alternative considered: retaining limited CLI composition for convenience; rejected because it weakens layering and recreates clone-like coupling.

4. Remove clone-specific hooks/config branches entirely instead of leaving inert fields.
   - Rationale: hard removal avoids dead configuration contracts and accidental future reuse.
   - Alternative considered: retaining `post-clone` config as ignored/no-op; rejected due to backward-compat burden and ambiguous UX.

## Risks / Trade-offs

- Users depending on `dml clone` lose a one-step workflow immediately -> Provide clear docs/tasks updates describing init + fetch/checkout alternatives.
- Clone code may share helpers with non-clone flows, creating accidental regressions during removal -> Refactor shared helpers first, then remove clone-only branches with targeted test updates.
- Thin-wrapper enforcement can surface hidden coupling in CLI modules -> Move orchestration into internal APIs and keep command handlers argument-only.
- Spec cleanup across multiple capabilities can miss references -> Use capability deltas for each impacted spec and ensure no clone requirements remain.

## Migration Plan

1. Update capability specs to delete clone requirements and codify init-only bootstrap + thin CLI constraints.
2. Remove clone command wiring and internal clone methods/ops; refactor any shared helpers needed by remaining commands.
3. Remove clone tests and rewrite affected assertions toward init + explicit remote workflow behavior.
4. Remove clone documentation and hook/config references; update CLI/help text to exclude clone.
5. Run full project test suites for CLI/internal ops and verify no clone paths remain reachable.

Rollback strategy: revert this change set as a whole if removal breaks critical workflows; partial rollback is discouraged because contract and implementation must stay aligned.

## Open Questions

- None; the change explicitly requires hard removal with no backward compatibility.
