## 1. Tighten Runtime Adapter Invocation

- [x] 1.1 Remove the `IndexOps` adapter import fallback and make missing concrete adapter commands fail closed.
- [x] 1.2 Preserve the existing builtin execution branch so `adapter == ""` remains valid only for explicit builtin-function handling.
- [x] 1.3 Audit runtime adapter invocation helpers and fixtures for any remaining symbolic adapter assumptions.

## 2. Resolve Sugar Before Runtime

- [x] 2.1 Ensure author-facing adapter sugar such as `local`, `lambda`, and plugin-defined symbolic names resolves to concrete adapter commands before runtime execution.
- [x] 2.2 Audit contrib executors and runnable constructors so resolved runnables carry canonical concrete adapter strings or explicit executable paths.
- [x] 2.3 Reject or eliminate raw runtime runnable construction patterns that still use symbolic adapter names.

## 3. Update Contracts And Tests

- [x] 3.1 Update contract and integration tests to require installed adapter console scripts instead of relying on import-based fallback.
- [x] 3.2 Add or revise tests covering non-`dml-` concrete adapter commands and explicit adapter paths.
- [x] 3.3 Add or revise tests covering the explicit builtin empty-adapter exception and rejection of empty adapters for non-builtin execution.
