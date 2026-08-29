## 1. Remove DmlOps clone surface

- [x] 1.1 Locate and delete `DmlOps.clone` implementations in all forms (sync/async/wrapper variants) and remove related exports.
- [x] 1.2 Update internal callers to use the surviving clone orchestration entrypoint directly, with no compatibility shim.
- [x] 1.3 Remove clone-only helpers that become unreachable after `DmlOps.clone` removal.

## 2. Rewire CLI clone routing

- [x] 2.1 Update clone CLI command handling to invoke one supported internal operations entrypoint directly after argument parsing.
- [x] 2.2 Ensure clone branch/tag flows preserve existing fetch/checkout semantics while avoiding `DmlOps.clone`.
- [x] 2.3 Update CLI-facing result mapping and error propagation for the new route.

## 3. Clean up and verify behavior

- [x] 3.1 Remove or rewrite tests that target `DmlOps.clone`, keeping clone behavior coverage at CLI/operation boundaries.
- [x] 3.2 Run clone-related unit/integration tests and fix regressions from the routing change.
- [x] 3.3 Update affected docs/comments to remove references to `DmlOps.clone` and reflect direct operation routing.
