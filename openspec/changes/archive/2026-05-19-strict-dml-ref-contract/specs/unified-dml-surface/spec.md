## ADDED Requirements

### Requirement: Shared `Dml` exact DB object contracts use `Ref`
The shared `Dml` surface SHALL require `Ref` objects for caller inputs that represent exact DB-backed objects, and it SHALL return `Ref` objects as the canonical identity for DB-backed objects in its payloads.

#### Scenario: Exact DAG access requires `Ref`
- **WHEN** a caller invokes a `Dml` method whose contract is to dereference an exact DAG object
- **THEN** the method requires a `Ref`
- **AND** it does not accept a plain `"dag:..."` string as a substitute

#### Scenario: Exact node access requires `Ref`
- **WHEN** a caller invokes a `Dml` method whose contract is to dereference an exact node object
- **THEN** the method requires a `Ref`
- **AND** it does not accept a plain `"node:..."` string as a substitute

#### Scenario: Non-DB selectors remain strings
- **WHEN** a caller provides a revision selector, DAG name, node name, branch, tag, remote URI, or `index_id`
- **THEN** the shared `Dml` surface continues to accept that value as a string

#### Scenario: DB-backed payloads use ref identity
- **WHEN** a shared `Dml` payload includes the identity of a commit, DAG, node, or other DB-backed object
- **THEN** that identity is represented by `Ref`
- **AND** the payload does not duplicate the same DB identity as a separate raw `id` string

## MODIFIED Requirements

### Requirement: `Dml` is the only fuzzy-selector boundary
The shared `Dml` class SHALL accept fuzzy selector strings only for workflows whose contract is lookup or repository navigation, and it SHALL require exact `Ref` objects for workflows whose contract is direct dereference or mutation of DB-backed objects.

#### Scenario: Revision selector resolves inside Dml
- **WHEN** a caller passes a supported revision string such as `HEAD~1` to a shared `Dml` repository method
- **THEN** the `Dml` method resolves it through the fuzzy-resolution submodule and lower-level ops receive only exact values

#### Scenario: DAG-name lookup resolves inside Dml
- **WHEN** a caller passes a DAG name to a shared `Dml` lookup workflow that documents name-based selection
- **THEN** the shared `Dml` method performs that selector resolution through the fuzzy-resolution submodule and lower-level ops do not parse that caller-facing form

#### Scenario: Exact DB-object workflow rejects fuzzy string grammar
- **WHEN** a caller passes a ref-like string such as `dag:abc123`, `node-literal:abc123`, or `commit:abc123` to a shared `Dml` workflow whose contract is for an exact DB-backed object
- **THEN** the method fails rather than coercing that string into a `Ref`

#### Scenario: Unsupported fuzzy grammar is rejected at Dml boundary
- **WHEN** a caller passes a selector form that is not documented by the redesigned CLI contracts
- **THEN** the shared `Dml` method fails rather than inventing additional grammar
