## Purpose
Define contract-testing taxonomy, canonical identifier practices, and migration guardrails so maintained tests stay non-duplicative, traceable, and fast-path friendly.

## Requirements

### Requirement: Contract-first test taxonomy
The repository SHALL organize maintained tests by contract intent with distinct locations for fast invariant checks and integration behavior, while allowing subsystem-owned roots to preserve the same taxonomy below the subsystem directory.

#### Scenario: Fast contract tests live under contracts taxonomy
- **WHEN** a test verifies a documented contract or invariant in isolation
- **THEN** it is placed under `tests/contracts/` or a subsystem-owned `contracts/` directory such as `tests/_core/contracts/`

#### Scenario: Integration tests live under integration taxonomy
- **WHEN** a test exercises multi-component behavior, external processes, remote roundtrips, or runtime orchestration
- **THEN** it is placed under `tests/integration/` or a subsystem-owned `integration/` directory such as `tests/_core/integration/`

#### Scenario: Core tests use subsystem-owned taxonomy
- **WHEN** a maintained test primarily targets `daggerml._core`
- **THEN** it is placed under `tests/_core/contracts/` or `tests/_core/integration/` according to contract or integration intent

### Requirement: Core tests are marker-selectable by subsystem
The repository SHALL mark tests collected under `tests/_core/` with a registered `core` pytest marker while keeping those tests included in default pytest selection.

#### Scenario: Core-only selection is available
- **WHEN** contributors run pytest with `-m core`
- **THEN** tests collected from `tests/_core/` are selected by the marker

#### Scenario: Core tests are skippable by marker
- **WHEN** contributors run pytest with `-m "not core"`
- **THEN** tests collected from `tests/_core/` are excluded by the marker

#### Scenario: Default selection includes core tests
- **WHEN** contributors run pytest without marker exclusion
- **THEN** tests collected from `tests/_core/` remain included by default

### Requirement: Core test migration preserves existing coverage content
The `_core` test reorganization SHALL preserve the existing test suite content during relocation.

#### Scenario: No tests are added during relocation
- **WHEN** `_core` tests are moved into `tests/_core/`
- **THEN** the change does not introduce additional test cases beyond the existing `_core` test cases

#### Scenario: No tests are deleted during relocation
- **WHEN** `_core` tests are moved into `tests/_core/`
- **THEN** every existing `_core` test case remains represented in the reorganized suite

#### Scenario: Test bodies preserve behavior
- **WHEN** `_core` tests are renamed or moved
- **THEN** their assertions, parametrization, generated-input bounds, and behavioral coverage remain unchanged

### Requirement: Core fixtures are local to the core test subtree
Shared fixtures and support objects used only by `_core` tests SHALL live under `tests/_core/` rather than in a top-level test fixture module.

#### Scenario: Core-specific helpers live in core subtree
- **WHEN** support code is specific to `_core` tests
- **THEN** it is placed in `tests/_core/helpers.py`, `tests/_core/strategies.py`, or `tests/_core/conftest.py`

#### Scenario: Core conftest owns moto server fixtures
- **WHEN** `_core` tests need S3-compatible AWS behavior
- **THEN** `tests/_core/conftest.py` provides fixtures backed by a moto `ThreadedMotoServer` endpoint

#### Scenario: Fake DML patches core export
- **WHEN** `_core` tests request the `fake_dml` fixture
- **THEN** the fixture patches `daggerml._core.Dml` rather than `daggerml.api.Dml`

### Requirement: Core test names avoid redundant subsystem prefixes
Maintained `_core` test names SHALL rely on file paths and module names for subsystem context and SHALL avoid repeating the file name or `core` prefix in each test function name.

#### Scenario: Test function name describes behavior only
- **WHEN** a `_core` test is moved under `tests/_core/`
- **THEN** its function name describes the tested behavior without repeating the `_core` subsystem or the source file name

#### Scenario: Importlib collection supports short names
- **WHEN** different `_core` modules contain similarly named behavior tests
- **THEN** pytest collection remains unambiguous because `--import-mode=importlib` and file paths identify each node

### Requirement: Canonical contract IDs are embedded directly in test identifiers
Each maintained contract-focused test SHALL include a canonical contract ID expressed as a direct literal string in test naming surfaces.

#### Scenario: Parameterized lifecycle case includes canonical ID
- **WHEN** a test case is defined in `pytest.mark.parametrize`
- **THEN** the case `id=` string includes the canonical contract ID followed by a human-readable case label

#### Scenario: Canonical IDs avoid indirection
- **WHEN** a test references a canonical contract ID
- **THEN** the ID is specified directly in the test or parameterized case definition and does not require a shared ID registry indirection

### Requirement: Lifecycle coverage uses parameterized stage matrices
Lifecycle-oriented contracts SHALL be tested with parameterized cases that explicitly represent each lifecycle stage.

#### Scenario: Lifecycle stages are represented as explicit parameterized cases
- **WHEN** a contract family spans kickoff, resume/poll, and terminal behavior
- **THEN** one parameterized test defines stage-specific cases with distinct IDs and assertions for each stage

#### Scenario: Stage-specific failures identify contract and stage
- **WHEN** a lifecycle parameterized case fails
- **THEN** the failure node identifier includes both canonical contract ID and stage label

### Requirement: Integration tests are marked slow
Integration tests SHALL be marked `@pytest.mark.slow` so they can be excluded from quick local runs.

#### Scenario: Integration test carries slow marker
- **WHEN** a test resides in the integration taxonomy or otherwise requires integration-level runtime behavior
- **THEN** the test is marked `@pytest.mark.slow`

#### Scenario: Fast test selection excludes integration tests
- **WHEN** contributors run `pytest -m "not slow"`
- **THEN** tests marked `slow` are excluded and the remaining suite represents the fast-path contract checks

### Requirement: Legacy test suite is fully migrated and superseded tests are removed
The repository SHALL complete migration of maintained tests to the contract matrix setup and SHALL remove superseded legacy tests to avoid duplicate maintenance.

#### Scenario: Superseded legacy tests are removed after parity
- **WHEN** a legacy test's contract coverage is represented by migrated contract-matrix tests
- **THEN** the legacy test is removed from maintained test paths

#### Scenario: End state contains only maintained tests aligned to taxonomy
- **WHEN** migration is complete
- **THEN** maintained tests conform to taxonomy, canonical ID, lifecycle parameterization, and slow-marker requirements defined in this specification

#### Scenario: Redundant parser smoke tests are removed once equivalent arg-level coverage exists
- **WHEN** a parser-creation smoke test duplicates parser argument assertions already maintained in the same suite
- **THEN** the redundant parser-creation smoke test is removed after parity verification

#### Scenario: Duplicate revision parsing checks are removed after central matrix adoption
- **WHEN** revision/ref/URI parsing forms are covered by the centralized parsing contract matrix
- **THEN** duplicate parsing checks in workflow-oriented contract tests are removed and workflow tests remain focused on operational invariants

#### Scenario: External-process orchestration tests are classified as slow
- **WHEN** a test requires subprocess execution, adapter polling loops, remote roundtrips, or equivalent runtime orchestration
- **THEN** the test is marked `slow` and excluded from `pytest -m "not slow"` selection

#### Scenario: Expensive adapter-path duplicates are collapsed into parameterized matrices
- **WHEN** multiple maintained tests exercise the same adapter-path contract family with near-identical setup and assertions
- **THEN** they are consolidated into one parameterized matrix suite that preserves canonical contract IDs and behavior-stage traceability

### Requirement: Migration ledger governs parity and removal
The repository SHALL track migration progress in a ledger that maps canonical contract IDs from legacy tests to migrated tests and records parity evidence before legacy removal.

#### Scenario: Batch plan records concrete suite order and risk
- **WHEN** migration planning is established
- **THEN** the ledger records bounded batch order with risk levels and exit criteria for each batch

#### Scenario: Contract mapping is explicit for each migrated suite
- **WHEN** a suite is selected for migration
- **THEN** the ledger records canonical contract IDs and old/new test file mappings for that suite

#### Scenario: Legacy test removal requires parity evidence
- **WHEN** a legacy suite is proposed for removal
- **THEN** the ledger includes passing evidence for targeted migrated suites, `pytest -m "not slow"`, and full `pytest` prior to removal

### Requirement: Core tests cover meaningful contracts only
Maintained tests for `daggerml._core` SHALL verify behavior that is contractually important, failure-prone, or concurrency-sensitive, and SHALL avoid trivial parser or delegation examples that do not exercise meaningful risk.

#### Scenario: Trivial parser examples are excluded
- **WHEN** a `_core` parser or selector test only demonstrates one obvious accepted string form without ambiguity, validation, or edge-case risk
- **THEN** the test is not included in the maintained `_core` contract suite

#### Scenario: Meaningful string contracts are included
- **WHEN** a `_core` string contract accepts a broad valid input space such as owner, project, branch, tag, ref name, revision selector, or remote root
- **THEN** the maintained tests include generated or matrix coverage for accepted edge cases and representative rejection cases

### Requirement: Core git-like repository workflows are covered at the correct test boundary
Maintained `_core` tests SHALL cover git-like repository workflows using the boundary that owns the contract under test: `Dml` for caller-facing local porcelain behavior, `CommitOps` for precise commit-graph and DAG-tree edge behavior, and `_core` integration tests for remote sync workflows.

#### Scenario: Local repository porcelain is tested through Dml
- **WHEN** maintained tests verify local git-like workflows such as `status`, `rev_parse`, `log`, `show`, `diff`, `checkout`, `merge`, `rebase`, `revert`, `dag.checkout`, or `dag.delete`
- **THEN** those tests live under `tests/_core/contracts/`
- **AND** they exercise the behavior through `Dml` unless the contract specifically depends on lower-level commit-graph setup that is clearer below the porcelain boundary

#### Scenario: Commit-graph edge behavior is tested through CommitOps
- **WHEN** maintained tests verify merge-base selection, first-parent ancestry walking, merge conflicts, revert conflicts, rebase replay, commit-relative diff behavior, or DAG-tree overwrite/delete edge cases
- **THEN** the tests MAY exercise `CommitOps` directly under `tests/_core/contracts/`
- **AND** they focus on exact graph and tree outcomes rather than re-testing `Dml` payload shaping

#### Scenario: Remote sync workflows are classified as integration behavior
- **WHEN** maintained tests verify `push`, `fetch`, or `pull`, or otherwise require remote protocol state, remote-tracking refs, or moto-backed S3 orchestration
- **THEN** those tests live under `tests/_core/integration/`
- **AND** they are marked `slow` according to the integration marker policy

### Requirement: Core git-like workflow tests avoid duplicate parsing matrices
Maintained `_core` git-like workflow tests SHALL rely on canonical valid revision inputs for operational assertions and SHALL avoid duplicating parsing-grammar breadth already owned by the centralized revision parsing matrix.

#### Scenario: Workflow test uses representative valid selector
- **WHEN** a checkout, merge, show, diff, fetch, pull, or push contract test needs a revision selector
- **THEN** the test uses one or more representative valid selector forms needed for that workflow
- **AND** it does not expand into a separate grammar matrix for branch, tag, commit, and URI parsing permutations already owned elsewhere

#### Scenario: Parsing breadth remains centralized
- **WHEN** maintainers need to add or adjust accepted and rejected selector forms for revision parsing
- **THEN** the maintained breadth-first matrix lives in the dedicated revision parsing contract suite
- **AND** git-like workflow suites remain focused on side effects, state transitions, and repository invariants

### Requirement: Core generated-input tests are bounded and fast
Generated-input tests for `daggerml._core` SHALL use Hypothesis only where generation improves confidence in accepted contract spaces, and SHALL bound examples and recursive shapes to preserve fast local feedback.

#### Scenario: Accepted input spaces use bounded strategies
- **WHEN** Hypothesis is used for `_core` ref names, project URIs, revision selectors, config values, or serde values
- **THEN** the strategy generates contractually accepted inputs with explicit bounds on examples, recursion, and collection sizes

#### Scenario: Recursive serde generation stays small
- **WHEN** DML serde round-trip tests generate nested values
- **THEN** generated values are bounded to finite scalars, string-keyed dictionaries, lists, refs, URIs, errors, and shallow runnable trees that keep the test fast

### Requirement: Core local concurrency contracts are tested
Maintained `_core` tests SHALL include local concurrency coverage for repository initialization, index creation, same-index mutation, branch commits, and reads during writes.

#### Scenario: Concurrent initialization produces one coherent repository
- **WHEN** multiple callers initialize the same project concurrently
- **THEN** the final repository has a valid DB, a valid `HEAD`, and a single coherent default branch state visible to all successful callers

#### Scenario: Concurrent runtime creation produces independent indexes
- **WHEN** multiple callers create runtime indexes against the same repository concurrently
- **THEN** each successful call returns a distinct valid index whose commit and DAG can be read through the typed DB facade

#### Scenario: Concurrent same-index distinct-name mutations preserve all names
- **WHEN** multiple callers mutate the same index concurrently with distinct node names
- **THEN** the final index DAG contains every distinct name and each named node is present in the DAG node set

#### Scenario: Concurrent same-index conflicting-name mutations remain coherent
- **WHEN** multiple callers mutate the same index concurrently with the same node name
- **THEN** the final name binding points to one valid returned node from a committed mutation and the index DAG remains readable and internally coherent

#### Scenario: Concurrent branch commits do not lose non-conflicting updates
- **WHEN** multiple callers commit non-conflicting DAG updates to the same attached branch concurrently
- **THEN** branch update serialization and merge behavior preserve all non-conflicting committed DAG names in the final branch history

#### Scenario: Concurrent reads during writes observe coherent state
- **WHEN** read operations run while other callers create indexes, mutate DAGs, or commit changes
- **THEN** each read observes either a coherent prior state or a coherent later state and never exposes a partial object graph or invalid pointer payload

### Requirement: Core execution coordination contracts are tested deterministically
Maintained `_core` execution-state tests SHALL cover same-cache-key coordination and CAS-style state updates without relying on broad slow adapter or network orchestration.

#### Scenario: Same-cache-key callers do not all launch work
- **WHEN** multiple callers attempt to start execution for the same cache key concurrently
- **THEN** at most one caller claims the launch path and other callers observe active or running coordination state instead of launching duplicate work

#### Scenario: Concurrent execution-record updates preserve spawned execution IDs
- **WHEN** multiple execution-state updates add or drop spawned execution IDs under CAS conflict conditions
- **THEN** bounded retry behavior preserves a coherent sorted set of spawned execution IDs or fails with an explicit contractually expected outcome

### Requirement: Public API contracts are tested with isolated Dml boundaries
Maintained tests for `daggerml.api` SHALL cover public wrapper contracts with isolated `Dml` fakes or mocks unless the behavior specifically requires a live repository.

#### Scenario: Default runtime helpers are contract-tested without live storage
- **WHEN** tests verify `get_default_dml`, `set_default_dml`, `clear_default_dml`, `use_default_dml`, `status`, `new`, `load`, or `temporary` wrapper behavior
- **THEN** the tests use mocked or fake `Dml` construction and namespace methods to verify resolution order, delegated calls, returned wrapper state, and user-facing errors without opening a live repository

#### Scenario: Dag wrapper behavior is contract-tested at namespace boundaries
- **WHEN** tests verify `Dag` methods such as `put`, named-node access, attribute assignment, `keys`, `values`, `argv`, `result`, `require`, `call`, `_call_builtin`, context-manager error capture, or `commit`
- **THEN** the tests assert public wrapper behavior and calls to `dml.runtime` / `dml.dag` using realistic `Ref` values and namespace return payloads

#### Scenario: Node wrapper behavior is contract-tested without repository internals
- **WHEN** tests verify `Node`, `RunnableNode`, `ListNode`, `DictNode`, or collection helper behavior
- **THEN** the tests assert wrapper return types, delegated builtin calls, concrete value loading, and documented exceptions without inspecting LMDB or `_core` object internals

### Requirement: Public API codec normalization is tested as an API contract
Maintained public API tests SHALL cover the literal codec registry and recursive normalization behavior exposed from `daggerml.api`.

#### Scenario: Codec plugin loading and ordering are deterministic
- **WHEN** codec tests exercise entry-point loading
- **THEN** they isolate codec global state, monkeypatch discovered entry points, and verify plugins load once with deterministic priority and registration ordering

#### Scenario: Codec errors preserve public error semantics
- **WHEN** a codec raises `DmlRepoError` during `apply_codec`
- **THEN** the original `DmlRepoError` is re-raised unchanged

#### Scenario: Non-repository codec failures are wrapped
- **WHEN** a codec raises a non-`DmlRepoError` exception during plugin loading or literal encoding
- **THEN** the public API raises `CodecError` with diagnostic context for the failing plugin or codec

#### Scenario: Recursive public value normalization is covered
- **WHEN** tests exercise `apply_codecs` on lists, dicts, `Uri`, `Runnable`, mappings, sequences, and `Node` values
- **THEN** the tests verify recursive normalization, same-index node ref reuse, committed cross-DAG node import, and rejection of uncommitted cross-index nodes

### Requirement: Public API integration tests use live Dml selectively
Maintained integration tests for `daggerml.api` SHALL use a live initialized `Dml` repository only for high-signal public workflows that cannot be fully trusted through mocks.

#### Scenario: Live workflow tests stay public-surface oriented
- **WHEN** an API integration test exercises a live repository
- **THEN** it drives the workflow through public API helpers and wrapper methods such as `new`, `put`, `commit`, `load`, `require`, collection helpers, and `use_default_dml` rather than asserting private storage layout

#### Scenario: Live API integration tests are classified as integration behavior
- **WHEN** a public API test initializes a repository, uses runtime orchestration, or depends on multi-component behavior
- **THEN** it lives under `tests/integration/` and is marked according to the repository marker policy for integration or live-runtime tests

#### Scenario: Live API integration does not duplicate core contract coverage
- **WHEN** a behavior is already covered by `_core` contract or integration tests
- **THEN** public API integration tests assert only the user-visible wrapper workflow needed to prove the API layer composes correctly with live `Dml`

### Requirement: Core rewrite removes superseded legacy tests after parity
The `_core` rewrite SHALL replace legacy `tests/_core/*` coverage with taxonomy-aligned contract and integration tests, and SHALL remove superseded legacy tests after parity is demonstrated.

#### Scenario: Legacy core tests are removed after contract parity
- **WHEN** a legacy `_core` test's meaningful behavior is represented by a new contract or integration test
- **THEN** the legacy test is removed rather than maintained in parallel

#### Scenario: Core rewrite preserves fast-path feedback
- **WHEN** contributors run the rewritten fast `_core` test selection on the maintainer machine
- **THEN** the selection completes in under 2 seconds, excluding tests that are explicitly classified as slow by the taxonomy
