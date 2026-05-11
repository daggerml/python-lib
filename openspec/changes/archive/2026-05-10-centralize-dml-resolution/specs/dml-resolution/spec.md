## ADDED Requirements

### Requirement: Revision resolution returns canonical commit refs
The DML resolution layer SHALL accept supported revision selectors, including direct commit refs, commit ids, `HEAD` ancestry selectors, branch names, and supported `dml://` revision URIs, and SHALL resolve them to a canonical commit `Ref`.

#### Scenario: Resolve a symbolic revision selector
- **WHEN** a caller resolves a supported symbolic revision selector such as `HEAD`, `HEAD~1`, a branch name, or a supported `dml://` URI
- **THEN** the resolution layer returns the corresponding commit `Ref`

#### Scenario: Reject an invalid revision selector
- **WHEN** a caller resolves a revision selector that is empty, malformed, or points to an unsupported object namespace
- **THEN** the resolution layer raises `DmlRepoError`

### Requirement: DAG resolution returns canonical dag refs
The DML resolution layer SHALL accept either a direct `dag:` ref or a DAG name combined with a revision selector and SHALL resolve the result to a canonical dag `Ref`.

#### Scenario: Resolve an explicit dag ref
- **WHEN** a caller resolves a selector that is already a valid `dag:` ref
- **THEN** the resolution layer returns that dag as a `Ref`

#### Scenario: Resolve a named dag from a revision
- **WHEN** a caller resolves a DAG name together with a commit-reachable revision selector
- **THEN** the resolution layer returns the dag `Ref` mapped to that name in the selected commit

#### Scenario: Reject incompatible dag inputs
- **WHEN** a caller provides an explicit `dag:` ref together with an incompatible revision override
- **THEN** the resolution layer raises `DmlRepoError`

### Requirement: Node resolution accepts direct refs, node-id selectors, and named lookups
The DML resolution layer SHALL accept node selectors as direct node refs, canonical node-id style selectors such as `node-literal:abc123`, or node names resolved through DAG context, and SHALL return a canonical node `Ref`.

#### Scenario: Resolve a direct node ref
- **WHEN** a caller resolves a selector that is already a valid node `Ref`
- **THEN** the resolution layer returns that node as a `Ref`

#### Scenario: Resolve a node-id style selector
- **WHEN** a caller resolves a selector string that matches a valid canonical node-id style selector
- **THEN** the resolution layer interprets it as a node `Ref` and returns it

#### Scenario: Resolve a named node lookup
- **WHEN** a caller resolves a node name together with sufficient DAG context
- **THEN** the resolution layer returns the named node as a `Ref`

### Requirement: Ambiguous node lookup requires dag disambiguation
The DML resolution layer MUST require an explicit DAG selector when a name-based node lookup cannot be resolved unambiguously from the available context, and it MUST fail with `DmlRepoError` instead of guessing.

#### Scenario: Reject ambiguous named node lookup
- **WHEN** a caller resolves a node name without a direct node ref or canonical node-id selector and the available context does not identify a single DAG
- **THEN** the resolution layer raises `DmlRepoError` instructing the caller to provide DAG context

#### Scenario: Allow unambiguous lookup without explicit dag selector
- **WHEN** a caller resolves a node name without a `dag_selector` and the available context identifies exactly one matching DAG
- **THEN** the resolution layer returns the matching node `Ref`

### Requirement: DML delegates selector resolution to the shared resolution layer
The `dml.py` orchestration layer SHALL use shared helpers from `dml_resolution.py` for commit, DAG, and node selector handling instead of implementing independent selector parsing logic.

#### Scenario: DML resolves a node selector
- **WHEN** DML code needs to resolve a node selector for a DAG operation
- **THEN** it uses the shared resolution layer and consumes the returned `Ref` rather than duplicating selector parsing rules locally
