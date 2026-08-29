## MODIFIED Requirements

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

## ADDED Requirements

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
