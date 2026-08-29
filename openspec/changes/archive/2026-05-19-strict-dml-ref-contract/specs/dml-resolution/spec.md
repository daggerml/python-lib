## MODIFIED Requirements

### Requirement: DAG resolution returns canonical dag refs
The DML resolution layer SHALL accept DAG lookup inputs only as a DAG name combined with a revision selector, and it SHALL resolve the result to a canonical dag `Ref`.

#### Scenario: Resolve a named dag from a revision
- **WHEN** a caller resolves a DAG name together with a commit-reachable revision selector
- **THEN** the resolution layer returns the dag `Ref` mapped to that name in the selected commit

#### Scenario: Reject explicit dag ref coercion input
- **WHEN** a caller passes a plain `"dag:..."` string to a DAG lookup resolver
- **THEN** the resolution layer raises `DmlRepoError` instead of coercing that string into a `Ref`

### Requirement: Node resolution accepts named lookups only
The DML resolution layer SHALL accept node lookup inputs only as node names resolved through DAG context or revision-reachable DAG discovery, and it SHALL return a canonical node `Ref`.

#### Scenario: Resolve a named node lookup
- **WHEN** a caller resolves a node name together with sufficient DAG context
- **THEN** the resolution layer returns the named node as a `Ref`

#### Scenario: Reject node-id style selector coercion
- **WHEN** a caller passes a plain string that matches a canonical node-id style selector such as `node-literal:abc123`
- **THEN** the resolution layer raises `DmlRepoError` instead of interpreting that string as a node `Ref`

### Requirement: Ambiguous node lookup requires dag disambiguation
The DML resolution layer MUST require an explicit DAG selector when a name-based node lookup cannot be resolved unambiguously from the available context, and it MUST fail with `DmlRepoError` instead of guessing.

#### Scenario: Reject ambiguous named node lookup
- **WHEN** a caller resolves a node name without explicit DAG context and the available context does not identify a single DAG
- **THEN** the resolution layer raises `DmlRepoError` instructing the caller to provide DAG context

#### Scenario: Allow unambiguous lookup without explicit dag selector
- **WHEN** a caller resolves a node name without a DAG selector and the available context identifies exactly one matching DAG
- **THEN** the resolution layer returns the matching node `Ref`

### Requirement: DML delegates selector resolution to the shared resolution layer
The `dml.py` orchestration layer SHALL use shared helpers from `dml_resolution.py` only for selector-to-ref lookup flows and SHALL bypass that layer for workflows that already require exact `Ref` inputs.

#### Scenario: DML resolves a lookup selector
- **WHEN** DML code needs to resolve a DAG name or node name for a lookup workflow
- **THEN** it uses the shared resolution layer and consumes the returned `Ref`

#### Scenario: DML bypasses resolution for exact ref input
- **WHEN** a `Dml` workflow already requires an exact `Ref` object
- **THEN** it validates that object directly instead of routing it through selector parsing helpers
