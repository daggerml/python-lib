## 1. Test Structure And Strategy Helpers

- [x] 1.1 Create the new `_core` contract/integration test file structure under `tests/contracts/` and `tests/integration/`.
- [x] 1.2 Add bounded Hypothesis strategies for valid ref segments, nested ref names, project URI parts, revision selectors, and bounded DML serde values.
- [x] 1.3 Add a small parallel execution helper for deterministic thread-based tests using barriers/events rather than sleeps.

## 2. Pure Core Contract Tests

- [x] 2.1 Add generated accepted-input tests for ref names, project URIs, branch/tag selectors, and path-safe head ref round-trips.
- [x] 2.2 Add revision selector tests for `HEAD`, `HEAD~n`, lowercase 64-character commits, explicit commit refs, local names, and project URIs.
- [x] 2.3 Add bounded DML serde round-trip tests for supported scalar, container, ref, URI, error, and runnable values.
- [x] 2.4 Add malformed serde envelope and unsupported-value tests for failure-prone decode/encode paths.
- [x] 2.5 Add focused type-system contract tests for namespace validation, object validation, DAG result/error invariants, and typed facade behavior using fake raw transactions.
- [x] 2.6 Add config contract tests for precedence, flatten/unflatten behavior, positive integer coercion, strict remote project validation, and remote root validation.

## 3. Local Concurrency Tests

- [x] 3.1 Add a parallel `Dml.init(...)` integration test proving concurrent initialization leaves one coherent project state.
- [x] 3.2 Add a parallel `runtime.create()` integration test proving concurrent index creation returns distinct readable indexes.
- [x] 3.3 Add a same-index distinct-name mutation integration test proving concurrent mutations preserve every name and named node.
- [x] 3.4 Add a same-index conflicting-name mutation integration test proving the final binding points to one returned valid node and the DAG remains coherent.
- [x] 3.5 Add a parallel branch commit integration test proving non-conflicting committed DAG names are preserved after serialized branch updates and merges.
- [x] 3.6 Add a reads-during-writes integration test proving status/log/runtime-list/DAG reads observe coherent states while writes are in progress.

## 4. Execution Coordination Tests

- [x] 4.1 Add deterministic fake CAS/remote support for execution-state tests without adapter subprocesses or network calls.
- [x] 4.2 Add same-cache-key coordination tests proving at most one concurrent caller claims the launch path.
- [x] 4.3 Add execution-record CAS conflict tests for spawned execution add/drop behavior and bounded retry outcomes.
- [x] 4.4 Decide whether a narrow moto-backed S3 conditional-write smoke test is necessary; add it only if fake CAS cannot cover an important production-specific behavior.

## 5. Legacy Replacement And Verification

- [x] 5.1 Map meaningful legacy `tests/_core/*` coverage to the new contract/integration files and identify trivial tests to drop without replacement.
- [x] 5.2 Remove superseded legacy `_core` tests after parity is represented by the new suite.
- [x] 5.3 Run targeted new `_core` contract and integration tests and record any production defects exposed by the new contracts.
- [x] 5.4 Run the fast-path selection and verify the rewritten fast `_core` coverage stays under the 2-second target on this machine.
- [x] 5.5 Run the repository test command required by contributor guidance, or document any remaining slow/external exclusions.
