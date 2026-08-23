## Purpose
Define how symbolic adapter names resolve to concrete command-line identities before execution.

## Requirements

### Requirement: Runtime adapter execution SHALL use only concrete command-line adapter identities
Any runnable that reaches adapter execution SHALL carry an adapter value that is directly command-line-callable from the runtime environment or is an explicit executable path. Runtime adapter execution SHALL NOT reinterpret that value as symbolic sugar, SHALL NOT consult the adapter registry to repair it, and SHALL NOT fall back to Python import-based `cli()` invocation.

#### Scenario: Built-in local adapter executes as a concrete command
- **WHEN** runtime execution receives a runnable with `adapter = "dml-local-adapter"`
- **THEN** it invokes `dml-local-adapter` as a command-line program
- **AND** it does not re-resolve `dml-local-adapter` through the adapter registry

#### Scenario: Plugin adapter executes without requiring a `dml-` prefix
- **WHEN** runtime execution receives a runnable with `adapter = "podman-adapter"`
- **THEN** it treats `podman-adapter` as a valid concrete adapter command if callable from the runtime environment
- **AND** it does not require the adapter string to start with `dml-`

#### Scenario: Explicit executable path is accepted
- **WHEN** runtime execution receives a runnable with `adapter = "/opt/acme/bin/build-adapter"`
- **THEN** it invokes that path directly as the adapter command

#### Scenario: Test adapter path must itself be executable
- **WHEN** a test or fixture passes a filesystem path such as `tests/assets/internal_fn/python-fork-adapter.py` as `runnable.adapter`
- **THEN** that file is expected to be directly executable by the runtime, including any required executable permission bits
- **AND** runtime execution does not repair a non-executable adapter path through Python import fallback

#### Scenario: Missing concrete adapter command fails closed
- **WHEN** runtime execution receives a runnable with a concrete adapter command that is not callable from the runtime environment
- **THEN** execution fails with an adapter-not-found error
- **AND** the runtime does not attempt Python import-based recovery

### Requirement: Symbolic adapter names SHALL resolve before runtime execution
Author-facing APIs MAY accept symbolic adapter names as sugar, but the adapter registry and runnable-resolution flow SHALL resolve that sugar to a concrete command-line adapter identity before runtime execution begins.

#### Scenario: Built-in sugar resolves to the canonical local adapter command
- **WHEN** author-facing code specifies `adapter = "local"`
- **THEN** runnable resolution produces a runtime runnable with `adapter = "dml-local-adapter"`

#### Scenario: Built-in sugar resolves to the canonical lambda adapter command
- **WHEN** author-facing code specifies `adapter = "lambda"`
- **THEN** runnable resolution produces a runtime runnable with `adapter = "dml-lambda-adapter"`

#### Scenario: Plugin-defined sugar resolves to a non-`dml-` adapter command
- **WHEN** a plugin registers symbolic adapter sugar `gpu`
- **THEN** runnable resolution MAY produce a runtime runnable with `adapter = "podman-adapter"`
- **AND** runtime execution treats that resolved command as canonical for that runnable

### Requirement: Builtin execution SHALL use an explicit empty-adapter exception
The runtime SHALL accept `adapter = ""` only in the explicit builtin-function branch where it detects builtin execution directly and does not shell out to an adapter process.

#### Scenario: Builtin function bypasses adapter execution
- **WHEN** runtime execution handles a builtin function such as `get` or `concat`
- **THEN** it uses the builtin execution branch instead of spawning an adapter process

#### Scenario: Empty adapter is not accepted for non-builtin execution
- **WHEN** a non-builtin runnable reaches adapter execution with `adapter = ""`
- **THEN** execution fails instead of treating the empty string as a command-line adapter identity
