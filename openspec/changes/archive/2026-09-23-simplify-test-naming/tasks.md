## 1. Baseline and Guidance

- [x] 1.1 Capture the pre-migration pytest collection inventory, including parametrized node IDs and total collected-test count.
- [x] 1.2 Update `CONTRIBUTING.md` with direct behavior naming, path-owned context, and meaningful class-grouping guidance.

## 2. Test Structure Migration

- [x] 2.1 Migrate root, API, and dashboard test names to direct behavior names and group related scenarios into focused classes where useful.
- [x] 2.2 Migrate contrib test names to direct behavior names and group related scenarios into focused classes where useful.
- [x] 2.3 Migrate core contract and integration test names to direct behavior names and group related scenarios into focused classes where useful.
- [x] 2.4 Preserve each existing marker, parametrized case, assertion, fixture behavior, and test-file location while restructuring.

## 3. Verification

- [x] 3.1 Compare post-migration pytest collection against the baseline and account for every renamed node ID and parametrized case.
- [x] 3.2 Run focused suites after each subsystem migration and resolve failures without deleting or weakening test coverage.
- [x] 3.3 Run the non-slow suite, full suite, lint, and type checks; remove temporary inventory artifacts after successful verification.
