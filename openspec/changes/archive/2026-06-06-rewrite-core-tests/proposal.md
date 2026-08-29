## Why

DaggerML's `_core` layer is expected to support highly parallel use: many DAGs may run concurrently, each DAG may launch multiple function executions and mutations, and users may modify repository state at the same time. The current `_core` tests are broad legacy coverage rather than a focused contract suite, and they do not clearly encode the tricky input, transaction, pointer, and execution-coordination guarantees that must hold under parallel load.

## What Changes

- Replace the maintained `_core` test suite with contract-focused tests under the current test taxonomy.
- Use Hypothesis for accepted input spaces where generation adds real confidence, including ref names, project selectors, revision selectors, and bounded DML serde values.
- Add narrowly scoped parallelism tests for local repository behavior, including concurrent initialization, concurrent index creation, concurrent same-index mutations, concurrent branch commits, and reads during writes.
- Add deterministic execution-coordination tests for same-cache-key locking and CAS-style state updates without broad slow remote orchestration.
- Remove superseded legacy `_core` tests once the new contract suite reaches parity for meaningful behavior.
- Preserve fast local feedback: the non-slow suite should stay under 2 seconds on the maintainer machine for the rewritten `_core` coverage.

## Capabilities

### New Capabilities

- None.

### Modified Capabilities

- `test-contract-matrix`: Add requirements for `_core` contract coverage, generated accepted-input tests, bounded performance, and concurrency-focused integration coverage.

## Impact

- Affected tests: `tests/_core/*` will be replaced by contract and integration tests under `tests/contracts/` and `tests/integration/`.
- Affected specifications: `openspec/specs/test-contract-matrix/spec.md` receives a delta for `_core` rewrite criteria.
- Dependencies: no new dependency is expected; `hypothesis` already exists in the dev dependency group.
- Runtime/API behavior: no intentional production behavior change, but the new tests may expose concurrency or stale-write defects that require implementation fixes in a later apply phase.
