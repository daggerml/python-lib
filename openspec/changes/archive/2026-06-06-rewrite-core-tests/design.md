## Context

The current `_core` tests live primarily under `tests/_core/` and mix parser examples, facade delegation tests, filesystem pointer tests, typed object tests, and DB-backed behavior. The suite gives useful coverage, but it does not read as a maintained contract matrix and does not emphasize DaggerML's expected parallel usage model.

The relevant `_core` behavior spans several boundaries:

- `head.py` owns filesystem pointers and an `fcntl`-backed repository lock.
- `types.py` owns typed object validation and the `DmlDB` facade over raw DB transactions.
- `index.py` mutates in-progress DAG state through serialized DB write transactions.
- `dml.py` coordinates branch commits through `Head.lock()` and `CommitOps.merge()`.
- `exec_state.py` coordinates same-cache-key execution through remote/CAS records.
- `uri.py`, `revision.py`, `serde.py`, and `config.py` contain pure contract logic that benefits from generated accepted-input tests.

The rewrite should be a test-suite change only. It may expose production defects during implementation, but production fixes should be driven by the failed contract and kept minimal.

## Goals / Non-Goals

**Goals:**

- Replace legacy `_core` tests with a small set of meaningful contract and integration tests.
- Encode accepted input spaces with bounded Hypothesis strategies where edge cases are likely: ref names, project URIs, revision selectors, and DML serde values.
- Add parallel tests for the usage model DaggerML must support: concurrent initialization, concurrent DAG/index creation, concurrent same-index mutations, concurrent branch commits, concurrent reads during writes, and same-cache-key execution coordination.
- Keep fast contract tests unmarked and keep the total rewritten `_core` suite fast enough for routine local feedback.
- Mark integration-level concurrency or remote-coordination tests according to the existing taxonomy.
- Remove superseded `tests/_core/*` tests once parity is represented by the new suite.

**Non-Goals:**

- Do not attempt broad line or branch coverage of `_core`.
- Do not preserve parser-smoke tests that only assert obvious examples such as one fixed URI form.
- Do not add new runtime dependencies.
- Do not use sleeps, polling loops, subprocess adapters, real network calls, or broad moto/S3 orchestration for ordinary contracts.
- Do not implement new concurrency behavior inside this proposal except where required to satisfy the new tests during apply.

## Decisions

### Use Contract Files Organized By Behavior

The rewrite should target behavior-named files under `tests/contracts/` and `tests/integration/`, rather than recreating `tests/_core/test_<module>.py` one-for-one.

Proposed contract files:

- `tests/contracts/test_core_head_refs.py`
- `tests/contracts/test_core_revision_selectors.py`
- `tests/contracts/test_core_serde_values.py`
- `tests/contracts/test_core_types_contracts.py`
- `tests/contracts/test_core_config_resolution.py`
- `tests/contracts/test_core_db_facade_contracts.py`

Proposed integration files:

- `tests/integration/test_core_parallel_init_integration.py`
- `tests/integration/test_core_parallel_runtime_integration.py`
- `tests/integration/test_core_parallel_branch_commits_integration.py`
- `tests/integration/test_core_execution_coordination_integration.py`

Alternative considered: keep `tests/_core/` and rewrite module-by-module. That would minimize file movement but keep the old taxonomy and encourage implementation-shaped tests rather than contract-shaped tests.

### Bound Hypothesis To Accepted Input Contracts

Hypothesis should generate only contractually accepted spaces unless a test is explicitly about rejection. Strategies should be small and local to the test area or a shared `tests/contracts/strategies_core.py` helper.

Key generated spaces:

- valid ref-name segments and nested ref names,
- valid project owner/project segments,
- valid branch and tag selectors,
- revision selectors including non-negative `HEAD~n`, 64-character lowercase hex commits, explicit commit refs, local names, and project URIs,
- bounded recursive serde values including finite scalars, lists, `dict[str, ...]`, `Ref`, `Uri`, `Error`, and shallow `Runnable` trees.

Use per-test `@settings(max_examples=25-50, deadline=None)` and bounded recursive strategies. This keeps tests useful without turning them into a fuzzing suite.

Alternative considered: deterministic examples only. That would be faster but would miss the string-shape edge cases that are most likely to break ref, URI, path quoting, and serde contracts.

### Treat Parallelism As A First-Class Contract

The suite should include targeted parallel tests with deterministic synchronization primitives rather than broad stress tests.

Expected local concurrency contracts:

- concurrent `Dml.init(...)` for the same project ends with one coherent repo state,
- concurrent `runtime.create()` calls produce distinct valid indexes,
- concurrent mutations to the same index are serialized by DB write transactions,
- concurrent same-index mutations with distinct names preserve all names and nodes,
- concurrent same-index mutations with the same name are last committed transaction wins for the name binding, with no corrupted graph,
- concurrent commits to the same branch are serialized by `Head.lock()` and merge non-conflicting changes,
- concurrent reads during writes observe coherent old or new states, never partial object graphs.

The same-index contract is especially important:

```
same index I
    ├── worker A: put_literal(..., name="a")
    └── worker B: put_literal(..., name="b")

final index I includes both names and both nodes
```

For name conflicts:

```
same index I
    ├── worker A: put_literal(..., name="x")
    └── worker B: put_literal(..., name="x")

final names["x"] is one returned valid node from the last committed transaction
```

Alternative considered: treat same-index concurrent mutation as unsupported. That does not match the expected library usage model.

### Keep Execution Coordination Tests Deterministic

Execution coordination should be tested through fake CAS/remote surfaces where possible, not through real adapter processes. The important contracts are one claimant for a same-cache-key launch, active/cache observation by other callers, bounded CAS retry behavior for spawned-execution updates, and preservation of execution record shape.

Alternative considered: use moto-backed S3 for all execution state tests. That is closer to production but risks becoming slow and flaky. A narrow moto test may remain if fake CAS cannot represent an important S3 behavior.

### Replace, Do Not Duplicate, Legacy Tests

Implementation should add new contract tests, verify parity, then remove superseded `tests/_core/*` files in the same change. Any retained legacy test must have a deliberate reason and should be migrated or renamed into the taxonomy.

Alternative considered: keep both suites temporarily. That increases maintenance cost and makes it unclear which tests define the intended contract.

## Risks / Trade-offs

- Concurrency tests can be flaky if they rely on timing → Use barriers/events, small worker counts, and deterministic assertions over returned refs and final object state.
- Real DB parallel tests can exceed the 2-second target → Keep worker counts small, avoid Hypothesis around DB-backed tests, and use one repo per integration scenario.
- New tests may expose existing stale-write behavior in `IndexOps` → Treat this as useful signal; apply minimal production fixes only after the contract is agreed.
- Fake execution-state stores may diverge from S3 behavior → Keep fake tests focused on CAS semantics and add at most one narrow integration test for S3-specific behavior if needed.
- Last-writer-wins conflicts can be scheduler-dependent → Assert only contractually stable properties: final name points to one valid returned node and graph state remains valid.

## Migration Plan

1. Add generated-input strategy helpers and pure contract tests.
2. Add local DB/filesystem parallel integration tests with small deterministic worker counts.
3. Add execution coordination tests using fake CAS/remote surfaces first.
4. Run targeted new tests and adjust contracts or implementation only where failures reveal real defects.
5. Remove superseded `tests/_core/*` tests after parity is represented.
6. Verify fast-path and full test commands according to contributor guidance.

## Open Questions

- Should integration-level concurrency tests under `tests/integration/` be marked `slow` even if they are designed to finish quickly, or should fast local-concurrency tests remain unmarked because they are core guarantees?
- Should conflicting same-index name writes retain all orphaned/unbound nodes in the DAG, or is it sufficient that the final name binding is valid and the graph is coherent?
- Is one fake CAS execution-coordination suite enough, or do we need one moto-backed smoke test for S3 conditional-write behavior?
