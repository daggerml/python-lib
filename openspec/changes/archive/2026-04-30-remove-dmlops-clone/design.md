## Context

Clone behavior currently spans CLI parsing, `DmlOps` workflow composition, and lower-level fetch/checkout operations. The `DmlOps.clone` layer is no longer providing unique behavior and instead proxies or re-composes logic that already exists elsewhere, which increases indirection and test maintenance. This change removes that layer while preserving observable clone behavior defined by existing specs.

Constraints:
- No backward compatibility shim for `DmlOps.clone`.
- Clone semantics must remain aligned with existing remote/fetch/checkout requirements.
- Related dead code should be removed in the same change to avoid partial cleanup.

## Goals / Non-Goals

**Goals:**
- Remove `DmlOps.clone` in all forms and all call sites.
- Route clone execution through supported internal operations directly.
- Keep user-facing clone behavior stable where requirements are unchanged.
- Reduce maintenance overhead by deleting clone-specific wrappers/helpers that become unused.

**Non-Goals:**
- Changing remote protocol semantics.
- Introducing new clone features.
- Preserving internal compatibility for code that imports or calls `DmlOps.clone`.

## Decisions

- Remove `DmlOps.clone` methods and update CLI routing to invoke the surviving operation path directly.
  - Rationale: keeps one authoritative clone composition path and removes duplicate orchestration logic.
  - Alternative considered: keep `DmlOps.clone` as a thin forwarding wrapper; rejected because it preserves dead abstraction and violates the no-shims requirement.

- Keep existing clone behavior assertions at CLI/operation boundaries, but relocate tests away from `DmlOps.clone` targets.
  - Rationale: protects behavior while allowing internal refactor/removal.
  - Alternative considered: broad test rewrite from scratch; rejected as unnecessary risk and effort.

- Remove clone-only helpers that become unreachable after method removal.
  - Rationale: avoid latent dead code and future confusion.
  - Alternative considered: defer cleanup to follow-up PR; rejected because dead code is directly caused by this change and should be removed atomically.

## Risks / Trade-offs

- [Risk] Hidden callers depend on `DmlOps.clone` internally. → Mitigation: run repository-wide reference search and update all call sites in the same change.
- [Risk] Behavioral regressions from routing updates. → Mitigation: retain/adjust existing clone integration tests and add targeted regression coverage where the call path changes.
- [Risk] Over-deletion of shared helpers. → Mitigation: remove only helpers proven clone-exclusive by static references and test coverage.

## Migration Plan

1. Remove `DmlOps.clone` definitions and exports.
2. Rewire CLI clone path to use direct operation composition.
3. Delete dead clone-specific helpers and obsolete tests.
4. Update and run clone-related unit/integration tests.
5. Update docs/comments that reference `DmlOps.clone`.

Rollback strategy:
- Revert this change as a unit if regressions are found, since no data migration is involved and the change is code-path/abstraction removal.

## Open Questions

- None currently; proceed with implementation based on existing clone/fetch/checkout requirements.
