# Task 07 - Rename final surfaces

## Goal

Rename transitional names that remained during rollout so the codebase reflects the final design cleanly.

## Current code anchors

- Transitional naming still exists today around pointer/local-manifest helpers in `src/daggerml/_internal/ops/remote.py:618`, `src/daggerml/_internal/ops/remote.py:625` and in caller sites such as `src/daggerml/_internal/ops/index.py:589`.
- Planning constraint: this task doc may reference those existing names, but this planning pass must not edit those code files. Only edit files under `docs/tasks/` here.

## Implement

- Rename any helper, test, fixture, or local variable names that still use transitional terminology when they now implement ref-manifest semantics.
- Apply the rename rule only to transitional/internal names. Do not rename stable surviving APIs whose final names are intentionally retained by the design.
- Transitional names to clean up include references to:
  - `pointer` or `ptr` when the value is specifically a manifest OID
  - `local manifest upload` when the code now publishes a ref-manifest
  - inline-sub-DAG terminology that no longer reflects the final design
- Update doc references to use the final names consistently.
- Keep behavior unchanged in this task; this is a naming cleanup after old flow removal.

## Inputs and outputs

- Inputs: existing code/tests/docs with transitional names.
- Outputs: final names aligned with the per-DAG manifest design.

## IO

- No behavior or remote IO changes.
- File edits only.

## Expected behavior to test

- No functional behavior changes from Task 06.
- Renamed tests still cover the same cases.
- Remaining docs and code references use the final per-DAG terminology consistently.
- Public or intentionally retained API names are unchanged unless explicitly removed in Task 06.

## Done when

- The codebase uses final names without carrying rollout-era terminology.
