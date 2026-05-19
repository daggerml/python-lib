# Remotes

Remotes let DaggerML share repository state and execution results outside the local LMDB store.

## The remote model

The current remote implementation uses S3 storage with two complementary layers:

- CAS for immutable objects, addressed by SHA-256 object id
- refs for discoverable names and mutable pointers

This split mirrors the local design:

- immutable content is stored by identity
- human or workflow-oriented names resolve through refs

## What gets published

Remote state includes a few different families of refs:

- project branch and tag refs for git-like sync
- DAG refs for per-DAG publication and discovery
- cache refs for function-result memoization

It also includes transport blobs under `io/invoke/` for adapter and executor boundaries.

At the top of the remote prefix, DaggerML stores a small `dml.json` descriptor describing the `cas+refs` layout. Under that layout, immutable payloads live in `cas/sha256/...`, discoverable names live under `refs/...`, and adapter transport or execution-coordination payloads live outside CAS in `io/...` and neighboring execution-state paths.

## Two remote roles

The docs and code distinguish two related remote concepts:

- `remote.root`: the storage/protocol root used for remote-backed execution and cache mutation
- `remote.project`: the project identity used for push, pull, fetch, and revision-style addressing

That means a runtime can use remote-backed execution features without necessarily being configured for full project sync.

## Sync and execution are related but not identical

Push and pull move repository state between local storage and the remote CAS-plus-refs layout.

Execution also depends on the remote when non-builtin adapters need shared cache and lifecycle state. In other words, the remote is both:

- a publication layer for repository history, and
- a coordination layer for distributed execution

## Integrity first

Remote operations validate object ids, manifests, ref payloads, and path rules. DaggerML treats malformed remote state as a hard failure rather than trying to guess what the data meant.

That validation includes the shape of manifest refs, the path rules for project branches and tags, and the one-segment cache-ref convention used for execution memoization.

See also:

- [Execution](execution.md)
- [Commits and history](commits-and-history.md)
- [Storage](storage.md)
