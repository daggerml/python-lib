## Why

Modules outside `daggerml._core` currently import `_core` implementation submodules, coupling extension code to runtime internals and weakening the intended application boundary. The boundary must be strict and mechanically protected: external namespaces may use `daggerml._core` only through its exported facade.

## What Changes

- Export `validate_adapter_response` from `daggerml._core` as the supported adapter-protocol validation entry point.
- Replace contrib imports of `_core` submodules with public package or `daggerml._core` facade imports.
- Replace contrib's direct `ExecutionState` use with public `Dml.runtime.read_execution_record(...)` inspection.
- Add an automated architecture contract that rejects `_core` submodule imports from every namespace outside `daggerml._core`.
- Remove the existing allowance for contrib to use private `_core` imports when no public equivalent exists; a missing facade must instead be added deliberately.

## Capabilities

### New Capabilities
- `core-import-boundary`: Defines and enforces the strict `daggerml._core` application boundary for all Python namespaces.

### Modified Capabilities
- `contrib-public-api-migration`: Requires contrib to use public facades without a private-import exception.
- `adapter-operation-protocol`: Exposes adapter-response validation through the supported `daggerml._core` facade.

## Impact

- Affects `src/daggerml/_core/__init__.py`, contrib adapters and executors, and architecture contract tests.
- Adds `validate_adapter_response` to the supported `daggerml._core` export surface.
- Does not change adapter wire payloads, response semantics, or execution lifecycle behavior.
