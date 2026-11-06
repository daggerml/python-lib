# DaggerML Core

`_core` owns the typed repository model, LMDB persistence, mutable DAG/runtime
construction, configuration, revisions, remote transport, execution state, and
cache coordination. `Dml` composes those services; `head.py`, `commit.py`,
`dag.py`, and `index.py` implement repository operations; `types.py`, `db.pyx`,
and `serde.py` persist typed objects; `remote.py` and `exec_state.py` coordinate
remote work.

This is an application boundary: other package namespaces import only deliberate
package-level exports. Normative storage, execution, remote, and lifecycle
contracts are owned by the relevant OpenSpec capabilities listed in
[`openspec/spec-overview.md`](../../../openspec/spec-overview.md).

Core runtime modules do not import the Python authoring API, contrib, or other
package namespaces.

## Execution records

Remote execution state is split by ownership. Immutable metadata records cache,
execution, and argv identity. Semantic state records lifecycle, result, lineage,
cancellation, and invalidation. Driver state records locking, opaque adapter
continuation, shared retry deadlines, and cleanup outcome. Cache pointers are
published only after these records exist and name the current execution attempt.
Legacy unified or partial split records are stale rather than migrated.

A terminal result is reusable when semantic state has a published result and no
cancellation or invalidation blocks it. Cleanup is coordinated independently:
pending, delayed, or failed cleanup does not invalidate a published result.
Cancellation conditionally removes the selected attempt's matching cache pointer
before adapter teardown, and `cancel-pending` prevents later result mutation.
Adapter scratch paths and caller edges remain separate from the execution record.

## Remote graph storage

The S3 descriptor is schema 1, layout `dagmanifest+dagpack+commits+refs+split-execution`.
`daggerml._core.get_remote_descriptor()` exposes a fresh copy of the canonical
descriptor for consumers such as the dashboard without sharing mutable state.
Immutable computation graphs are deterministic gzip JSON packs rooted at a DAG
(`packs/dag/<id>.json.gz`) or an argument node (`packs/argv/<id>.json.gz`). Both
use the same schema-1 envelope: `root` is the full typed ref; `objects` maps typed
refs to tagged JSON object payloads. Each pack contains its root's own body,
stopping at function-DAG edges and all import edges (including selected nodes).
It contains no commit history, mutable indexes, or external artifact bytes.
Small shared body objects may occur in several packs.

Uploads stop at an existing graph manifest without inspecting that graph's local
body or descendants. For a published function used by new work, its flattened
manifest dependencies are reused rather than traversing the function body.
New bodies are partitioned once and encoded from those same objects. Packs and
imported manifests are durable before a dependent manifest is published, so a
manifest is a completeness boundary; an isolated pack from an interrupted upload
is not.

Graph manifests (`manifests/<namespace>/<id>.json`) carry the root, flattened
function sub-DAG refs, and imported-DAG pointers. Import manifests are resolved
before any bodies are downloaded. Once the complete plan is known, every body
whose full graph is already available locally is filtered out, including function
and imported DAGs. Their dependency edges are read locally for validation; an
existing root with a missing descendant or datum is not considered complete.
Function bodies enter one continuous global
download queue; a bounded completion queue feeds caller-side decoding and
replayable LMDB writes without transport batch barriers. Root/import bodies are
materialized only after function bodies and dependency validation. A failed
fetch may leave validated function objects locally, but never publishes refs;
partial roots are checked for complete closure before reuse.

`commits/<id>.json` embeds a commit, its tree, and the DAG-name-to-pack-root map.
Commit uploads stop at an existing remote commit manifest without reading that
commit, its tree, or older ancestors locally. New parent manifests are published
before their children. Forced/new-ref publication still checks local ancestry
when the repository has shallow boundaries. Parent refs remain history edges.
A shallow fetch downloads selected manifests
and their complete graph dependencies without reading omitted parents. Ref publication
follows durable pack and manifest writes and retains conditional branch/tag
updates. Imports validate closure and every object identity in a replayable local
transaction before publishing local refs. Only byte transport runs in workers.

Execution arguments use argv packs; cached and finished results use DAG packs.
Execution metadata/state/driver records, locks, cache pointers, refs, scratch
files, and external artifacts retain their existing independent representations.

Remote GC traces commit ancestry and graph/import manifests from project and
retained execution roots, reading each reachable manifest once per run. It marks
the referenced physical keys and batch-deletes unmarked packs and manifests.
It does not open a local database, read pack bodies, verify graph identities, or
check that referenced packs exist. Unpublished uploads and ref tombstones have a
`prune_age_seconds` grace period (24 hours by default). Run remote GC with sync,
execution publication, and other writers quiescent: it is not a distributed
snapshot or a lock against concurrent publication. Objects under the immutable
prefixes that DaggerML did not write (including S3 console folder markers) fail
GC with their URI by default; `unrecognized="ignore"` (`--unrecognized ignore`)
logs each URI and leaves it untouched. URI artifact bytes remain
outside graph-pack collection.
The existing `cas-deleted`/`cas-retained` summary counters now count physical
packs, graph manifests, and commit manifests; retained counts include uploads in the grace period.

`python -m daggerml._core.migrate_remote` is the offline legacy-CAS conversion
tool. It writes only an empty, non-overlapping destination; copies refs,
execution records, and auxiliary objects verbatim; converts all reachable
project/argv/result graphs; re-downloads and validates them; checks the source
inventory for changes; then publishes the new descriptor last. Unchanged records,
including adapter state, locks, cleanup status, partial records, and cache
pointers, are preserved without lifecycle validation or cleanup. Writers must
remain stopped throughout migration and cutover. Dry-run performs metadata checks, not
a complete graph verification. Failure leaves an unpublished staging prefix and
never rolls back or deletes source data. Retry with a new empty destination.
