## 1. Init Contract

- [x] 1.1 Remove `name`-based init inputs from Python and CLI entrypoints and accept only optional `remote_project` and optional `remote_root`.
- [x] 1.2 Enforce init validation that rejects configured `remote_project` when `remote_root` is absent.
- [x] 1.3 Update init bootstrap behavior so fetch/checkout runs only when `remote.project` is configured.

## 2. Local Config And Capability Gates

- [x] 2.1 Update shared config and local project-config helpers to allow missing local `remote.project` while preserving branchless validation when present.
- [x] 2.2 Add explicit project-sync guards so push, pull, fetch, and related flows fail with targeted errors when `remote.project` is absent.
- [x] 2.3 Preserve `remote.root` as the required capability for remote-backed mutation and execution paths.

## 3. Tests And Docs

- [x] 3.1 Replace name-derived init tests with coverage for local-only init, `remote_root`-only init, and `remote_project`-without-`remote_root` rejection.
- [x] 3.2 Add coverage proving project sync commands fail when `remote.project` is absent while remote-backed mutation flows remain allowed with only `remote.root`.
- [x] 3.3 Update configuration, init, and remote-sync documentation to describe the new capability split.
