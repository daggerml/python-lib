## Context

The CLI currently acts as both transport layer and workflow coordinator in places, blending input/output concerns with domain decision-making. This creates duplicated branching, makes command behavior harder to validate in isolation, and increases the risk of interface regressions when internal logic evolves. The repository already has layered boundaries (CLI -> API/internal ops), so this change formalizes that boundary for maintainability and testability.

## Goals / Non-Goals

**Goals:**
- Ensure CLI command handlers only perform argument parsing, call into domain interfaces, and serialize results/errors.
- Move non-transport decision logic out of CLI modules into API/internal layers with explicit contracts.
- Keep user-visible CLI semantics and output shape stable unless a compatibility fix is explicitly required.
- Make behavior testable at the correct layer (domain behavior tested outside CLI; CLI tests focused on parsing and formatting).

**Non-Goals:**
- Redesigning command names, flags, or broad UX flows.
- Rewriting underlying domain behavior unrelated to boundary extraction.
- Introducing a new CLI framework.

## Decisions

### Decision: Define CLI as a thin interface boundary
The CLI will be treated as a transport adapter with three responsibilities: parse inputs, invoke one domain entrypoint, serialize outputs.

Alternatives considered:
- Keep selective orchestration in CLI for convenience: rejected because boundary remains ambiguous and hard to enforce.
- Push all behavior into CLI-specific helper utilities: rejected because it only relocates, not resolves, layering concerns.

### Decision: Move branching/workflow rules to API or internal ops based on ownership
If logic expresses user-level behavior contract, place it in public API modules; if it reflects transactional/domain primitives, place it in internal ops.

Alternatives considered:
- Move all logic directly to internal ops: rejected because API-level semantics and ergonomics still need a stable home.

### Decision: Standardize command result envelopes before formatting
Command handlers should consume structured domain results and apply consistent output serialization paths (success, validation failure, execution failure).

Alternatives considered:
- Keep per-command ad hoc output shaping: rejected due to inconsistency and duplicated error translation.

### Decision: Enforce boundary with tests and code review checks
Update tests so CLI tests assert parsing/serialization only, while behavior tests move to API/internal suites. Add review guidance to prevent reintroducing orchestration logic into CLI paths.

Alternatives considered:
- Rely on convention without tests: rejected because drift is likely over time.

## Risks / Trade-offs

- [Risk] Extracting logic may accidentally change edge-case command behavior -> Mitigation: capture baseline behavior with regression tests before and after extraction.
- [Risk] Refactor can temporarily duplicate logic across layers -> Mitigation: perform iterative moves per command area with cleanup checkpoints.
- [Risk] Error mapping changes may alter exit codes/message text -> Mitigation: preserve and assert current externally visible error contract in CLI-focused tests.
- [Trade-off] More explicit interfaces between CLI and domain layers increase initial verbosity -> Mitigation: gain long-term clarity and lower maintenance overhead.

## Migration Plan

1. Inventory CLI commands and identify non-transport logic currently in handlers.
2. Define/confirm target domain entrypoints for each command area.
3. Extract one command area at a time, preserving existing output and exit code behavior.
4. Move/add tests to validate behavior at domain layers and keep CLI tests transport-focused.
5. Remove dead CLI branches/helpers once each area is migrated.

Rollback strategy:
- Revert per-command extraction commits if contract regressions are discovered, then re-apply with stronger regression coverage.

## Open Questions

- Should a lightweight lint/check be added to flag disallowed imports or patterns in `src/daggerml/_cli/**`?
- Are there any intentionally CLI-only behaviors that should remain exceptions to the thin-boundary rule?
