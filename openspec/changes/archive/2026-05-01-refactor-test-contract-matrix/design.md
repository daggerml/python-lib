## Context

The repository currently has broad test coverage but inconsistent structure: many tests are organized by module history instead of contract boundary, lifecycle assertions are often split across multiple test functions, and integration-heavy tests are not consistently marked for selective execution. This makes the suite harder to navigate, raises maintenance cost, and weakens traceability from documented requirements to test intent.

The change introduces a full-suite migration to a contract matrix approach:

- tests organized around contract surfaces,
- canonical contract IDs embedded directly in test naming and parameterized case IDs,
- lifecycle stage coverage expressed via parameterization,
- integration tests consistently marked `slow`,
- legacy superseded tests removed.

Pressure-test findings from the current repository state:

- There are currently no tests marked `@pytest.mark.slow`.
- CI currently runs full unfiltered test passes (`pytest .`) in both standard and sanitizer jobs.
- Autouse fixture coupling is present in top-level and internal test conftest modules, which can blur contract vs integration boundaries during migration.
- Lifecycle helper logic (for example argv manifest setup and poll-until-terminal loops) is duplicated across multiple contrib suites and should be consolidated as part of lifecycle matrix migration.

## Goals / Non-Goals

**Goals:**
- Make each maintained test target a single documented contract or invariant.
- Improve traceability from docs/specs to test failures using stable canonical IDs.
- Reduce lifecycle test duplication via parameterized stage matrices.
- Make quick local feedback dependable with `-m "not slow"`.
- Complete migration of maintained tests to the new setup and eliminate legacy duplicates.

**Non-Goals:**
- Changing product runtime behavior or public API semantics.
- Introducing a centralized contract-ID registry module.
- Rewriting every helper fixture unless needed for speed/clarity boundaries.

## Decisions

1. Test taxonomy is contract-first with explicit fast/integration split.
   - Decision: Use `tests/contracts/` for fast invariant checks and `tests/integration/` for multi-component/infrastructure tests.
   - Rationale: Improves discoverability and makes speed characteristics obvious from location.
   - Alternative considered: Keep current folders and annotate intent with comments/markers only. Rejected because structure would remain ambiguous and drift-prone.

2. Canonical IDs are literal strings at point-of-use.
   - Decision: Put IDs directly in test function names and parameterized `id=` strings (for example `EXEC-LC-003:resume-uses-launch-state`).
   - Rationale: Preserves readability and avoids indirection overhead while still enabling stable traceability.
   - Alternative considered: Central CONTRACT map or registry module. Rejected for now due to maintenance overhead and limited immediate benefit.

3. Lifecycle assertions are represented as parameterized stage matrices.
   - Decision: For lifecycle-heavy surfaces, collapse near-duplicate tests into one parameterized test per contract family.
   - Rationale: Reduces duplication and makes missing lifecycle stages visible as absent cases.
   - Alternative considered: Keep separate functions per stage. Rejected because behavior drift across stages is harder to detect.

4. Integration selection is marker-driven and mandatory.
   - Decision: Mark integration tests `@pytest.mark.slow` and enforce this in migrated suites.
   - Rationale: Aligns with existing pytest marker configuration and contributor workflow for fast iteration.
   - Alternative considered: Path-only selection without marker discipline. Rejected because existing paths include mixed-speed tests during migration.

5. Migration is replacement, not long-term parallel tracks.
   - Decision: Remove superseded legacy tests as contract-matrix equivalents land.
   - Rationale: Avoids dual maintenance and contradictory assertions across old/new structures.
   - Alternative considered: Keep old tests indefinitely for safety. Rejected because it increases noise and slows suite evolution.

## Risks / Trade-offs

- [Risk] Migration churn causes temporary CI instability due to file moves/renames.
  → Mitigation: Migrate in bounded batches by subsystem with parity checks before deletion.

- [Risk] Inconsistent canonical ID formatting across contributors.
  → Mitigation: Define and enforce formatting in a single taxonomy doc and apply review checks.

- [Risk] Over-parameterized tests become hard to read.
  → Mitigation: Keep one contract family per parameterized test and use explicit readable case IDs.

- [Risk] Some tests are hard to classify as contract vs integration.
  → Mitigation: Default uncertain cases to integration + `slow`, then optimize toward contract tests when isolation is straightforward.

- [Risk] Fixture refactors may reveal hidden integration coupling in historically fast tests.
  → Mitigation: Introduce explicit fixture scopes and remove implicit autouse integration setup where it blurs boundaries.

## Migration Plan

1. Publish taxonomy and naming conventions in docs.
2. Create initial contract/integration directory structure.
3. Execute migration in bounded batches with parity checkpoints:
   - Batch 1 (low risk): contract-focused suites with minimal infrastructure coupling.
   - Batch 2 (medium): lifecycle-heavy local runtime and funkify suites.
   - Batch 3 (medium-high): execution-state and internal roundtrip integration-heavy suites.
   - Batch 4 (high): ssh-backed and remaining infrastructure-heavy integration suites.
4. Apply `@pytest.mark.slow` to integration suites during migration.
5. Remove superseded legacy tests only after parity evidence is captured in the migration ledger.
6. Update contributor guidance and CI invocation expectations to match marker usage.

### Initial Migration Ledger (Batch 1)

Batch 1 target suites and planned contract mapping:

- `tests/contrib/test_executor_base.py` -> `tests/contracts/contrib/executor/test_executor_base_handle.py`
  - `EXB-HDL-001`: start path when `state=None`
  - `EXB-HDL-002`: poll path when state exists
  - `EXB-HDL-003`: terminal start result passthrough
  - `EXB-HDL-004`: mixed state invocations route correctly
- `tests/contrib/test_ssh_executor.py` ->
  - `tests/contracts/contrib/executor/test_ssh_resolve_runnable.py`
    - `SSH-RES-001`, `SSH-RES-002`
  - `tests/contracts/contrib/executor/test_ssh_handle.py`
    - `SSH-HDL-001` through `SSH-HDL-005`
- `tests/test_default_runtime.py` -> `tests/contracts/runtime/test_default_runtime_status.py`
  - `DRT-STS-001` through `DRT-STS-004`

Batch 1 parity gate before legacy removal:

- targeted migrated suites pass,
- `pytest -m "not slow"` pass,
- full `pytest` pass,
- contract mapping and removal decision recorded in ledger.

Rollback strategy:
- If migration introduces instability, pause after a completed subsystem batch; retain migrated layout and restore confidence by fixing tests, rather than restoring legacy duplicates.

## Open Questions

- Should we introduce a custom pytest marker (for example `contract`) in a follow-up to enable direct contract-only selection?
- Do we want an automated check that every `tests/integration/**` test is marked `slow`?
- Should canonical contract IDs be validated via lint rule in a later iteration?
