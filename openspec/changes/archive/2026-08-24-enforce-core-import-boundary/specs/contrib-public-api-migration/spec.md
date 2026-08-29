## MODIFIED Requirements

### Requirement: Contrib SHALL use existing public APIs where sufficient
Contrib modules SHALL use public `daggerml.api`, package-root, or `daggerml._core` facade exports for Dml sessions, DAG wrappers, node wrappers, public value wrappers, adapter protocol contracts, DAG creation, loading, temporary sessions, default-runtime access, and runtime inspection. Contrib modules MUST NOT import `daggerml._core` implementation submodules. When required behavior lacks a public equivalent, the owning boundary SHALL expose a deliberate public operation or facade export instead of allowing contrib to bypass the boundary.

#### Scenario: Public value wrapper is sufficient
- **WHEN** contrib code needs public value wrappers such as `Runnable`, `Uri`, `Ref`, or `Error`
- **THEN** it SHALL import them from the public API or package root instead of private `_core` modules

#### Scenario: Runtime inspection is required
- **WHEN** contrib code needs a stored execution record or published result ref
- **THEN** it SHALL inspect the execution through the public `Dml.runtime` API
- **AND** it SHALL NOT instantiate or import the private execution-state implementation

#### Scenario: Required core contract lacks a facade export
- **WHEN** contrib requires a core-owned cross-boundary contract that is not publicly exposed
- **THEN** the contract SHALL be deliberately exported through the owning public facade before contrib uses it
- **AND** contrib SHALL NOT import its defining `_core` submodule
