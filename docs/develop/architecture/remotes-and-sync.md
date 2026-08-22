# Remotes and Sync

`Remote` in `_core/remote.py` maps the local typed object graph to an S3-backed
transport layout. It is not a second local repository. A remote contains a
descriptor, immutable SHA-256-addressed CAS blobs, typed project refs, plain
cache-to-execution pointers, and unified execution records.

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
concurrent remote changes; non-forced tags are create-only. Execution cache
pointers use `exec/cache/` keyed by normalized function arguments.

Branch and tag inspection can read typed commit tips directly from either the
main endpoint or a dependency endpoint. This path validates a present descriptor
without initializing a missing one, limits descriptorless emptiness detection to
one-key existence probing, and reads only the selected ref namespace. It does
not traverse CAS, materialize commits, or update local tracking pointers.

Execution coordination is adjacent to, but separate from, CAS and project refs.
`ExecutionState` stores embedded locks and attempt state in unified records,
with separate lineage edges and adapter IO. Cancellation uses those locks to
order caller-edge publication against the CAS transition to `cancel-pending`,
then CAS-transitions adapter-cleaned attempts to `canceled`. Remote CAS garbage
collection traces project refs plus execution-record `argv_ref` and `result_ref`
roots.

The shared surface exposes cache reads and invalidation through `Dml.cache`.
Garbage collection is one top-level workflow: `Dml.gc()` computes local roots
from HEAD, local refs, fetched refs, dependencies, and live runtime indexes,
while `Dml.gc(remote=True)` delegates to maintenance for configured
`remote.root`. Import-only dependency endpoints are never GC targets.
