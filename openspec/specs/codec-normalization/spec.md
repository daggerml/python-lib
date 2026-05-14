## ADDED Requirements

### Requirement: Codec logic has a single owning module
The system SHALL define `src/daggerml/codecs.py` as the only module that contains codec logic, codec types, codec registry behavior, plugin loading behavior, and built-in codec implementations.

#### Scenario: Internal callers import codec behavior from the unified module
- **WHEN** internal staging code needs codec registration or codec application behavior
- **THEN** it imports that behavior from `daggerml.codecs`
- **AND** `daggerml._internal.*` does not define codec logic of its own

#### Scenario: Built-in codecs live in the unified module
- **WHEN** the system provides built-in codec behavior for `Node` values or delayed-action values
- **THEN** those codec implementations are defined in `daggerml.codecs`

### Requirement: Stage 1 preserves current codec call semantics
During Stage 1, the system SHALL continue to invoke codec behavior from internal staging call sites using `CodecContext`, while sourcing that behavior from `daggerml.codecs`.

#### Scenario: Literal staging still applies codecs through internal call sites
- **WHEN** `_internal` literal staging normalizes a value during Stage 1
- **THEN** it applies codecs through `daggerml.codecs`
- **AND** it passes `CodecContext` to codec `encode(...)`

#### Scenario: Function staging still applies codecs through internal call sites
- **WHEN** `_internal` function staging normalizes argv or kwargv values during Stage 1
- **THEN** it applies codecs through `daggerml.codecs`
- **AND** it passes `CodecContext` to codec `encode(...)`

#### Scenario: Codec-local failures are translated at the internal boundary
- **WHEN** codec application fails during Stage 1
- **THEN** `daggerml.codecs` raises a codec-local error type
- **AND** the `_internal` caller translates that failure into the repository-domain error surface it already exposes

### Requirement: Stage 2 codecs receive Dag instances
During Stage 2, the codec plugin contract SHALL pass `daggerml.api.Dag` into codec `encode(...)` instead of `CodecContext`.

#### Scenario: Built-in codec receives Dag
- **WHEN** a built-in codec encodes a value during Stage 2
- **THEN** its `encode(...)` method receives the active `Dag` instance

#### Scenario: Plugin codec receives Dag
- **WHEN** a plugin codec loaded from the `daggerml.codecs` entry-point group encodes a value during Stage 2
- **THEN** its `encode(...)` method receives the active `Dag` instance

### Requirement: Dag owns recursive codec normalization in Stage 2
During Stage 2, `daggerml.api.Dag` SHALL own recursive codec normalization and insertion for values accepted by public staging and call-entry methods.

#### Scenario: Dag.put normalizes recursively before runtime staging
- **WHEN** `Dag.put(value)` is called during Stage 2
- **THEN** `Dag` recursively applies codecs and normalizes nested values before delegating to runtime literal staging

#### Scenario: Dag.call inserts callable and arguments before execution
- **WHEN** `Dag.call(fn, *args, **kwargs)` is called during Stage 2
- **THEN** `Dag` inserts the callable, positional arguments, and keyword argument values through the codec-driven normalization path before invoking runtime function staging

#### Scenario: Node remains a codec during Dag-owned normalization
- **WHEN** a `Node` value is encountered during Stage 2 normalization
- **THEN** the system handles it through the built-in `Node` codec rather than through a special non-codec rule

### Requirement: Codec plugins remain discoverable through the existing entry-point group
The system SHALL continue to load codec plugins from the `daggerml.codecs` entry-point group across both migration stages.

#### Scenario: Entry-point group remains stable
- **WHEN** codec plugins are discovered after this change
- **THEN** discovery uses the `daggerml.codecs` entry-point group
- **AND** plugin loading preserves deterministic ordering and re-encode behavior
