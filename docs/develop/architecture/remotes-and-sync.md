# Remotes and Sync

`Remote` in `_core/remote.py` maps the local typed object graph to an S3-backed
transport layout. It is not a second local repository. A remote contains a
descriptor, immutable SHA-256-addressed CAS blobs, and JSON refs for projects,
tags, DAG manifests, caches, active execution ownership, and cancellation
targets.

Publishing walks the local object closure from a root ref, uploads missing CAS
objects, and writes a manifest ref. Child DAG boundaries remain explicit rather
than being recursively flattened into one transport object. Fetching validates
and materializes a selected manifest closure into the local database before
writing a tracking pointer. Local branches persist an upstream branch name.

Each remote root contains one project, with direct `refs/heads/*` and
`refs/tags/*` transport paths. Local project tracking is under
`.dml/refs/remote/{heads,tags}`, while import-only dependencies use
`.dml/refs/dep/<name>/{heads,tags}`. Revision strings never include endpoint
identity; callers select remote or dependency tracking with source flags. Branch
updates check ancestry and use conditional S3 writes to avoid silently replacing
concurrent remote changes; non-forced tags are create-only. Cache publication
uses `refs/cache/` keyed by normalized function arguments.

Branch and tag inspection can read typed commit tips directly from either the
main endpoint or a dependency endpoint. This path validates a present descriptor
without initializing a missing one, limits descriptorless emptiness detection to
one-key existence probing, and reads only the selected ref namespace. It does
not traverse CAS, materialize commits, or update local tracking pointers.

Execution coordination is adjacent to, but separate from, CAS and project refs.
`ExecutionState` stores locks, lifecycle records, launch state, lineage, and
adapter I/O under its execution prefix. Remote cleanup likewise has two scopes:
transport scratch data can be pruned, while CAS garbage collection traces from
published refs as roots.
