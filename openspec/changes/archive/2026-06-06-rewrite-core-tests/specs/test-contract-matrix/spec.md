## ADDED Requirements

### Requirement: Core tests cover meaningful contracts only
Maintained tests for `daggerml._core` SHALL verify behavior that is contractually important, failure-prone, or concurrency-sensitive, and SHALL avoid trivial parser or delegation examples that do not exercise meaningful risk.

#### Scenario: Trivial parser examples are excluded
- **WHEN** a `_core` parser or selector test only demonstrates one obvious accepted string form without ambiguity, validation, or edge-case risk
- **THEN** the test is not included in the maintained `_core` contract suite

#### Scenario: Meaningful string contracts are included
- **WHEN** a `_core` string contract accepts a broad valid input space such as owner, project, branch, tag, ref name, revision selector, or remote root
- **THEN** the maintained tests include generated or matrix coverage for accepted edge cases and representative rejection cases

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

### Requirement: Core rewrite removes superseded legacy tests after parity
The `_core` rewrite SHALL replace legacy `tests/_core/*` coverage with taxonomy-aligned contract and integration tests, and SHALL remove superseded legacy tests after parity is demonstrated.

#### Scenario: Legacy core tests are removed after contract parity
- **WHEN** a legacy `_core` test's meaningful behavior is represented by a new contract or integration test
- **THEN** the legacy test is removed rather than maintained in parallel

#### Scenario: Core rewrite preserves fast-path feedback
- **WHEN** contributors run the rewritten fast `_core` test selection on the maintainer machine
- **THEN** the selection completes in under 2 seconds, excluding tests that are explicitly classified as slow by the taxonomy
