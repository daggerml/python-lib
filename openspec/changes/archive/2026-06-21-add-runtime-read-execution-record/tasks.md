## 1. Runtime API surface

- [x] 1.1 Add `dml.runtime.read_execution_record(execution: Ref | str)` to `_RuntimeNamespace` and normalize `Ref | str` inputs to an execution-id string before delegation.
- [x] 1.2 Reuse the existing execution-state reader so the new method preserves `remote.root` requirements and missing-record failure behavior.

## 2. Contracts and verification

- [x] 2.1 Add or update runtime-surface contract tests to cover reads by `Ref` and by `str`, asserting that the returned payload is the raw execution record typed dict with no reshaping.
- [x] 2.2 Add or update contract coverage for missing execution records to confirm the method surfaces the underlying `DmlRepoError` behavior unchanged.
