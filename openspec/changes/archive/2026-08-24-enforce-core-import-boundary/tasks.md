## 1. Public Core Facade

- [x] 1.1 Export `validate_adapter_response` from `daggerml._core` and cover the facade export contract.
- [x] 1.2 Migrate contrib response types and validators to direct public facade imports.
- [x] 1.3 Replace `contrib.adapters` use of `ExecutionState` with public `Dml.runtime.read_execution_record(...)` inspection while preserving nested cleanup.

## 2. Boundary Enforcement

- [x] 2.1 Add an AST-based architecture contract that rejects `_core` submodule imports from non-core modules under `src/daggerml` while allowing direct facade imports.
- [x] 2.2 Update contrib adapter tests for the public runtime-inspection path and verify invoke polling still performs nested cleanup with the published result ref.

## 3. Documentation And Verification

- [x] 3.1 Document the strict core import boundary and the public adapter-response validator in the relevant architecture and extension references.
- [x] 3.2 Run focused boundary and contrib tests, then complete the repository's required typecheck, lint-fix, and non-slow test checks.
