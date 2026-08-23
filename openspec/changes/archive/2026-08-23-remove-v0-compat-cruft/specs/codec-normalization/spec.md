## MODIFIED Requirements

### Requirement: Stage 2 codecs receive Dag instances
The codec plugin contract SHALL pass the active `daggerml.api.Dag` into codec `encode(...)`. `CodecContext` and staged compatibility call semantics SHALL NOT be part of the current codec contract.

#### Scenario: Built-in codec receives Dag
- **WHEN** a built-in codec encodes a value
- **THEN** its `encode(...)` method receives the active `Dag` instance

#### Scenario: Plugin codec receives Dag
- **WHEN** a plugin codec loaded from the `daggerml.codecs` entry-point group encodes a value
- **THEN** its `encode(...)` method receives the active `Dag` instance

### Requirement: Dag owns recursive codec normalization in Stage 2
`daggerml.api.Dag` SHALL own recursive codec normalization and insertion for values accepted by public staging and call-entry methods.

#### Scenario: Dag.put normalizes recursively before runtime staging
- **WHEN** `Dag.put(value)` is called
- **THEN** `Dag` recursively applies codecs and normalizes nested values before delegating to runtime literal staging

#### Scenario: Dag.call inserts callable and arguments before execution
- **WHEN** `Dag.call(fn, *args, **kwargs)` is called
- **THEN** `Dag` inserts the callable, positional arguments, and keyword argument values through the codec-driven normalization path before invoking runtime function staging

#### Scenario: Node remains a codec during Dag-owned normalization
- **WHEN** a `Node` value is encountered during normalization
- **THEN** the system handles it through the built-in `Node` codec rather than through a special non-codec rule

### Requirement: Codec plugins remain discoverable through the existing entry-point group
The system SHALL load codec plugins from the `daggerml.codecs` entry-point group using the sole current Dag-owned codec contract.

#### Scenario: Entry-point group remains stable
- **WHEN** codec plugins are discovered
- **THEN** discovery uses the `daggerml.codecs` entry-point group
- **AND** plugin loading preserves deterministic ordering and re-encode behavior

## REMOVED Requirements

### Requirement: Stage 1 preserves current codec call semantics
**Reason**: The temporary `CodecContext` migration stage is complete and retaining it as an active requirement creates a phantom second codec protocol.
**Migration**: None. Built-in and plugin codecs implement the current Dag-owned codec contract directly.

## RENAMED Requirements

- FROM: `Stage 2 codecs receive Dag instances`
- TO: `Codecs receive Dag instances`
- FROM: `Dag owns recursive codec normalization in Stage 2`
- TO: `Dag owns recursive codec normalization`
