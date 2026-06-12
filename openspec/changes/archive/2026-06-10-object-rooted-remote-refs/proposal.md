## Why

The current remote model is more complicated than the local data model. Remote refs point at manifests, manifests carry synthetic closure data, and `dag` and `commit` are treated as special traversal cases. That adds redundant structure, creates drift between code and specs, and already leaves liveness holes around nested objects.

We want the remote to look like the repository: refs point at typed objects, and object payloads carry the graph.

## What Changes

- **BREAKING**: Replace manifest-rooted remote refs with simple typed remote refs that store `ref.to`, `created`, and `metadata`.
- **BREAKING**: Remove manifest closure semantics from push, pull, and remote GC. Remote graph traversal follows actual stored object refs recursively.
- Standardize remote ref families around root object type instead of transport-specific special cases:
- `transport` and `cache` refs point to `dag`
- `active` refs point to `node-argv`
- project branch and tag refs point to `commit`
- tombstones are the original refs moved unchanged to the tombstone location
- Keep the remote CAS as the shared object store keyed by object id.
- Keep this intentionally simple: no backward compatibility, no manifest shims, no version bumps, and no redundant validation layers beyond root-type checks where needed.
- Keep implementation scoped to `src/daggerml/_core/remote.py`; if the change appears to require code changes outside that module, stop and review rather than expanding scope.

## Capabilities

### New Capabilities

- `remote-object-refs`: Define the object-rooted remote ref payload and recursive push/pull/liveness model.

### Modified Capabilities

- `remote-project-refs`: Project refs now publish typed commit pointers instead of manifest targets.
- `runtime-execution-records`: Active, cache, and transport refs now use typed object pointers with fixed root namespaces.

## Impact

- Affected code: `src/daggerml/_core/remote.py`.
- Affected tests: remote push/pull, remote GC, execution remote-state tests, and any contract coverage asserting manifest fields or other manifest-shaped remote payloads.
- Affected specifications: the three capabilities listed above.
- Compatibility: backward compatibility is intentionally not preserved.
