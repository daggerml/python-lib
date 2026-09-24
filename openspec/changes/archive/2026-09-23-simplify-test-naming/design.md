## Context

See `proposal.md` for motivation. The suite currently mixes behavior names with names that encode package scope, test taxonomy, and contract identifiers. Directory paths already provide package and taxonomy context under pytest's import-mode collection. Related tests are largely module-level even when they share setup and a common subject.

## Goals / Non-Goals

**Goals:**

- Make collected pytest node IDs concise and understandable without decoding naming prefixes.
- Make the module path the sole source of package and test-taxonomy context.
- Make related scenarios visually and structurally discoverable through focused test classes.
- Prove the migration preserves the collected test and parametrized-case inventory.

**Non-Goals:**

- Adding lifecycle coverage or changing product behavior.
- Changing the directory taxonomy, pytest markers, or test selection defaults.
- Rewriting test logic, fixtures, assertions, or coverage targets except where a move into a class requires mechanical receiver changes.

## Decisions

### Use direct behavior names

Test methods and functions will use `test_<behavior>` names, such as `test_can_deepen_history`. Names will not repeat information in their module path, including subsystem, `contract`/`integration` taxonomy, or numeric identifiers.

Alternatives considered:

- Retain contract IDs in function names. Rejected because pytest output becomes opaque and the IDs duplicate requirement tracking better held in specs or parametrized case IDs.
- Use a generic verb-object grammar everywhere. Rejected because natural direct behavior names are clearer and need less policy.

### Group by shared subject or scenario

Tests covering one public subject, lifecycle, or fixture-backed scenario will be grouped in `Test<Subject>` classes. Class fixtures will be used when they make scenario setup explicit and reusable; unrelated tests remain module-level rather than being forced into arbitrary classes.

Alternatives considered:

- Convert every module to one class. Rejected because it adds nesting without exposing a meaningful boundary.
- Keep all tests module-level. Rejected because shared setup and related behavior remain difficult to scan.

### Preserve test inventory before optimizing structure

The migration will inventory collected node IDs and parametrized cases before editing, then compare collection counts and execute the full suite afterward. Renames and class moves will be behavior-preserving; any failure discovered during execution is recorded and resolved separately rather than silently deleting or weakening a test.

Alternatives considered:

- Rename opportunistically while adding lifecycle scenarios. Rejected because it makes coverage-loss review and regressions difficult to isolate.

### Keep file paths stable for this migration

Existing file and directory locations remain stable. Module imports and path selection retain their current meaning while function names and class structure change.

Alternatives considered:

- Rename every test file alongside tests. Rejected because that expands selector churn without advancing the naming goal.

## Risks / Trade-offs

- [Node IDs change] -> Preserve path and marker selection, document the compatibility impact, and compare the pre/post collection inventory by count and case coverage.
- [Class fixtures accidentally broaden state] -> Prefer function-scoped fixtures and only share immutable or explicitly reset scenario setup.
- [Large mechanical diff hides a lost case] -> Migrate in bounded modules, run collection after each area, and retain an inventory artifact until full verification passes.
- [Existing contract traceability is lost] -> Keep canonical IDs in OpenSpec artifacts and parametrized `id=` values when they identify matrix cases; remove them from ordinary function names.

## Migration Plan

1. Capture baseline collection counts and node IDs, including parametrized cases.
2. Update contributor guidance before or alongside the first migrated module so the new convention is explicit.
3. Migrate test modules in bounded subsystem groups, grouping related cases and extracting only fixtures that clarify shared setup.
4. After each group, compare collection output and run its focused tests.
5. Run the fast suite and full suite; investigate failures without dropping tests.
6. Remove the temporary baseline inventory after verification, unless a maintained inventory is useful for CI.

Rollback is a normal revert of the migration commit(s); no runtime data or external protocol changes occur.
