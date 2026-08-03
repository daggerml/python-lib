# Remotes and Sync

`Remote` in `_core/remote.py` maps the local typed object graph to an S3-backed
transport layout. It is not a second local repository. A remote contains a
descriptor, immutable SHA-256-addressed CAS blobs, and JSON refs for projects,
tags, DAG manifests, caches, active execution ownership, and cancellation
targets.

Publishing walks the local object closure from a root ref, uploads missing CAS
objects, and writes a manifest ref. Child DAG boundaries remain explicit rather
than being recursively flattened into one transport object. Fetching enumerates
a named project's refs, validates and materializes each manifest closure into
the local database, then writes local tracking pointers under that remote name.
Local branches persist a separate upstream mapping to one remote tracking branch.

Project transport uses `dml://owner/project#branch` and `@tag` selectors. Local
tracking uses `remote/branch` and `remote@tag` selectors. Branch
updates check ancestry and use conditional S3 writes to avoid silently replacing
concurrent remote changes; non-forced tags are create-only. Cache publication
uses `refs/cache/` keyed by normalized function arguments.

Execution coordination is adjacent to, but separate from, CAS and project refs.
`ExecutionState` stores locks, lifecycle records, launch state, lineage, and
adapter I/O under its execution prefix. Remote cleanup likewise has two scopes:
transport scratch data can be pruned, while CAS garbage collection traces from
published refs as roots.
