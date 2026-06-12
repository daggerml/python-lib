## ADDED Requirements

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
