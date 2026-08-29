## Context

The internal contract test tree currently mixes three concerns: command wiring, workflow invariants, and parser grammar validation. Multiple files repeat parser-smoke assertions and revision/URI form checks, especially in CLI setup tests and git-like workflow contracts. This redundancy increases churn and broadens failure blast radius when parsing changes.

The repository's testing taxonomy requires contract-first coverage, lifecycle parameterization, and removal of superseded tests once parity is confirmed. The change must preserve behavior coverage while reducing duplication and making ownership of parsing contracts explicit.

## Goals / Non-Goals

**Goals:**
- Centralize revision/ref/URI parsing behavior into one parameterized contract matrix with canonical IDs.
- Prune redundant parser smoke tests that do not add unique invariants.
- Reclassify external-process orchestration tests as `slow` where taxonomy and runtime behavior indicate integration-level execution.
- Reduce fast-path runtime by collapsing duplicate expensive adapter-path tests into one parameterized contract matrix per behavior family.
- Keep workflow tests focused on operational invariants and delegation boundaries.
- Maintain fast contract-suite ergonomics and traceable migration decisions.

**Non-Goals:**
- Changing runtime parsing behavior in production code.
- Reorganizing integration test layout or slow-marker policy.
- Broadly renaming unrelated tests for style-only reasons.

## Decisions

1. **Introduce a single parsing contract owner suite**
   - Create one contract-focused suite for revision/ref/URI parsing forms and errors.
   - Use parameterized case matrices to encode form variants and failure boundaries.
   - Rationale: this reduces duplicate assertions and aligns with lifecycle/matrix guidance.

2. **Treat parser-smoke setup tests as removable when covered by specific parser arg tests**
   - Delete `test_parser_creation` tests in files where per-subcommand arg tests already assert equivalent parser wiring.
   - Rationale: avoid duplicate maintenance and preserve high-signal tests.

3. **Move parsing assertions out of workflow-oriented git-like contract tests**
   - Relocate revision classification and URI canonicalization checks from workflow files into the parsing matrix.
   - Keep state-transition and delegation checks in workflow files.
   - Rationale: separates grammar contracts from behavior contracts and narrows regression scope.

4. **Retain user-visible CLI behavior checks even if lightweight**
   - Preserve tests asserting output format/newline behavior and key top-level help sentinel coverage.
   - Rationale: these are contract-relevant UX boundaries, not parser duplication.

5. **Apply slow-marker policy to external-process execution paths**
   - Mark tests that require adapter subprocess execution, polling loops, or runtime orchestration as `slow`.
   - Keep pure in-memory contract checks in the fast path.
   - Rationale: aligns test selection semantics with taxonomy and reduces non-slow wall time.

6. **Collapse expensive adapter path duplicates into one matrix per contract family**
   - Replace near-duplicate adapter execution tests with parameterized cases that preserve contract IDs and stage labels.
   - Keep one high-signal representative per unique behavior boundary.
   - Rationale: maintain contract parity while cutting repeated setup/execution overhead.

## Risks / Trade-offs

- **Risk:** Over-pruning may remove subtle edge-case coverage hidden in mixed workflow tests. -> **Mitigation:** move first, then delete only after parity matrix includes those cases.
- **Risk:** Centralized parsing matrix can become too broad and hard to read. -> **Mitigation:** group parameterized cases by surface (`parse_ref`, URI canonicalization, revision resolution) with clear IDs.
- **Risk:** Case-ID traceability may regress during migration. -> **Mitigation:** use direct canonical contract IDs in parameterized `id=` labels.
- **Trade-off:** Fewer local parser smoke tests means less immediate locality in some CLI files. -> **Mitigation:** keep command-level wiring tests and document parsing ownership in test naming.
- **Risk:** Moving tests to `slow` may hide regressions in default local runs. -> **Mitigation:** retain at least one fast representative per contract family and enforce full CI coverage with `slow` included.
- **Risk:** Matrix collapsing can accidentally drop edge variants. -> **Mitigation:** build explicit case inventory before deletion and verify parity from old-to-new mapping.

## Migration Plan

1. Add the new parsing contract matrix suite with all migrated parsing scenarios.
2. Move/port parsing assertions from CLI/base, GC helper parsing, and git-like workflow contract tests.
3. Identify external-process adapter/runtime orchestration tests in fast path and mark qualifying tests as `slow`.
4. Collapse duplicate expensive adapter-path tests into parameterized matrices with canonical IDs.
5. Remove redundant parser-smoke tests with demonstrated parity.
6. Run fast contract suite and targeted integration checks to confirm no behavioral coverage loss and improved non-slow runtime.

## Open Questions

- Should GC `parse_heads` remain partially local as a command contract, or be fully centralized with other parsing checks?
- Should lightweight parser smoke tests in status/config/contrib be removed now or in a follow-up cleanup once matrix migration lands?
- Which adapter-path tests must remain fast representatives versus being reclassified `slow` to satisfy both contract confidence and local-loop performance?
