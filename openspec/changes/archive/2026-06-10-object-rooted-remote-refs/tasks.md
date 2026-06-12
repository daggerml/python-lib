## 1. Remote Ref Payload Simplification

- [x] 1.1 Replace manifest-shaped remote ref payloads with simple typed pointer payloads storing `ref.to`, `created`, and `metadata`.
- [x] 1.2 Remove manifest-specific read/write/materialization helpers from remote sync code.

## 2. Recursive Object Traversal

- [x] 2.1 Rework push to traverse the local object graph recursively from the root typed ref and upload missing CAS objects before publishing the remote ref.
- [x] 2.2 Rework pull to start from `ref.to` and materialize reachable objects recursively until the local DB already contains them.
- [x] 2.3 Rework remote GC to mark liveness by traversing real stored objects from published refs instead of synthetic closure fields.

## 3. Ref Family Root Types

- [x] 3.1 Enforce stable root namespaces for project, cache, transport, and active refs.
- [x] 3.2 Keep `active` rooted at `node-argv` and stop treating completed DAGs as active roots.
- [x] 3.3 Make tombstones move the original ref payload to the tombstone path unchanged.

## 4. Implementation Boundary

- [x] 4.1 Implement the change entirely in `src/daggerml/_core/remote.py`.
- [x] 4.2 If implementation appears to require changes outside `remote.py`, stop and review before proceeding.

## 5. Verification

- [x] 5.1 Update contract coverage for raw remote ref payload shape, recursive push/pull behavior, and remote GC liveness.
- [x] 5.2 Update execution remote-state tests to assert `active -> node-argv`, `cache -> dag`, and `transport -> dag`.
