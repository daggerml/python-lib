## ADDED Requirements

### Requirement: is_node_like predicate exists in contrib API
The system SHALL provide a public function `is_node_like(x)` in `daggerml.contrib.api` that returns `True` if and only if `x` is an instance of `Node`, `DelayedRef`, `DelayedLoad`, or `DelayedRunnable`.

#### Scenario: Node instance is node-like
- **WHEN** `is_node_like(x)` is called with a `Node` instance
- **THEN** it returns `True`

#### Scenario: DelayedRef is node-like
- **WHEN** `is_node_like(x)` is called with a `DelayedRef` instance
- **THEN** it returns `True`

#### Scenario: DelayedLoad is node-like
- **WHEN** `is_node_like(x)` is called with a `DelayedLoad` instance
- **THEN** it returns `True`

#### Scenario: DelayedRunnable is node-like
- **WHEN** `is_node_like(x)` is called with a `DelayedRunnable` instance
- **THEN** it returns `True`

#### Scenario: Plain value is not node-like
- **WHEN** `is_node_like(x)` is called with a plain Python value (str, int, list, None, etc.)
- **THEN** it returns `False`

#### Scenario: DelayedActionCodec is not node-like
- **WHEN** `is_node_like(x)` is called with a `DelayedActionCodec` instance (the internal codec wrapper)
- **THEN** it returns `False`

### Requirement: SshExecutor uses is_node_like for field validation
`SshExecutor._validate_kw` SHALL use `is_node_like` to accept node-like values for the `host` and `flags` fields instead of checking `isinstance(x, DelayedActionCodec)` directly.

#### Scenario: Node-like host passes validation
- **WHEN** `_validate_kw` is called with `host` set to a `Node`, `DelayedRef`, `DelayedLoad`, or `DelayedRunnable`
- **THEN** validation passes without error

#### Scenario: Node-like flags passes validation
- **WHEN** `_validate_kw` is called with `flags` set to a `Node`, `DelayedRef`, `DelayedLoad`, or `DelayedRunnable`
- **THEN** validation passes without error

#### Scenario: Invalid host still raises error
- **WHEN** `_validate_kw` is called with `host` set to an empty string or a non-node-like non-string
- **THEN** a `DmlRepoError` is raised
