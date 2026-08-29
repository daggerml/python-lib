## Purpose

Define a strict application boundary that keeps `daggerml._core` implementation modules private while exposing intentional cross-namespace contracts through its package facade.

## ADDED Requirements

### Requirement: Non-core modules SHALL NOT import core implementation modules
Every source module whose qualified namespace is outside `daggerml._core` MUST NOT import `daggerml._core` submodules. Such modules MAY import names exposed directly by the `daggerml._core` package facade or by another public DaggerML API.

#### Scenario: External namespace uses the core facade
- **WHEN** a module outside `daggerml._core` needs a contract exported by `daggerml._core`
- **THEN** it imports that contract directly from `daggerml._core`
- **AND** it does not import the implementation submodule that defines the contract

#### Scenario: Core implementation composes its own submodules
- **WHEN** a module within the `daggerml._core` namespace needs another core implementation module
- **THEN** the boundary permits that internal import

### Requirement: Core import boundary SHALL be mechanically enforced
The repository SHALL include an automated architecture contract that inspects DaggerML source-module imports and fails when a module outside `daggerml._core` imports a `daggerml._core` submodule.

#### Scenario: Forbidden submodule import is introduced
- **WHEN** a source module outside `daggerml._core` imports `daggerml._core.<submodule>`
- **THEN** the architecture contract fails and identifies the offending module and import

#### Scenario: Direct facade import is inspected
- **WHEN** a source module imports a name directly from `daggerml._core`
- **THEN** the architecture contract accepts the import
