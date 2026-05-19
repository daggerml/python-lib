# Remote Protocol

DaggerML's remote layer is implemented in `src/daggerml/_internal/ops/remote.py` and `src/daggerml/_internal/exec_state.py`. It uses S3 as a content-addressed store plus a small ref namespace.

## The remote surface

When `RemoteOps` initializes, it expects or creates a `dml.json` descriptor describing the `cas+refs` layout. Under that prefix, the important areas are:

- `cas/sha256/<aa>/<bb>/<oid>`: immutable content-addressed objects,
- `refs/tags/**`, `refs/cache/**`, and `refs/projects/**`: published refs,
- `refs/dags/<dag_id>.json`: per-DAG manifests used to recurse across DAG boundaries,
- `exec/**` and `io/**`: execution-state and adapter-transport data.

The remote side is not a second copy of the local repo layout. It is a transport-oriented layout designed around manifests, refs, and immutable blobs.

The descriptor currently declares a schema version, the `sha256` hash family, and the canonical `refs`, `io`, and `cas/sha256` prefixes. That gives clients one stable description of how to interpret a remote root before they start reading any manifests or refs.

## Manifest and ref shape

The remote protocol revolves around two small JSON object families:

- manifests, which name a root object plus the closure needed to materialize it
- refs, which publish a manifest under a discoverable path such as a branch, tag, cache key, or DAG id

In practice, refs also carry just enough metadata for the runtime to preserve direct DAG relationships and execution provenance. Cache refs record the execution id that published them, branch refs distinguish mutable heads from immutable tags, and per-DAG refs let nested DAG boundaries stay visible instead of collapsing into one giant closure.

## Push in plain terms

Pushing means taking a local root object, describing the closure needed to reconstruct it, uploading any missing CAS blobs, then publishing a ref that points at the manifest.

For branch and tag sync, the root is a commit. For cache publication, the root is usually a DAG. In both cases `RemoteOps`:

1. walks the local object graph into a local manifest,
2. derives the direct child DAG ids for that manifest layer,
3. uploads missing CAS blobs after verifying SHA-256,
4. writes per-DAG refs for child DAG manifests as needed,
5. publishes the top-level ref with `targets` metadata for the direct DAG ids.

That direct-DAG metadata is how the remote side keeps nested DAG relationships visible without flattening the entire graph into one manifest.

## Pull in plain terms

Pulling starts from a remote ref, not from an object id guessed by the client.

`RemoteOps` reads the ref JSON, validates it, loads the target manifest, and then materializes the closure into the local database. If the manifest mentions child DAG ids, it follows `refs/dags/*.json` to load those manifests too. The implementation uses a thread pool so independent manifests and CAS objects can be fetched concurrently, but the resulting objects are still materialized into one local transaction path.

At the end of a branch or tag fetch, DaggerML writes a local tracking pointer rather than treating the remote state as separately mounted storage.

## Project sync vs cache sync

The same remote machinery supports two different user-facing stories.

### Project sync

Project sync uses canonical `dml://owner/project#branch` and `@tag` URIs. `RemoteOps` validates the URI pieces with `revision_uri.py`, maps them onto `refs/projects/...`, and then enforces branch or tag semantics:

- branch refs can be updated conditionally,
- non-fast-forward branch pushes are rejected unless `force` is set,
- tag refs are immutable once created.

### Cache sync

Cache sync uses `refs/cache/<cache_key>.json`. The cache key comes from the argv datum id, which means the remote cache is keyed by the normalized execution input graph rather than by a separate ad hoc hash layer in Python.

## Execution metadata

`ExecutionState` adds a second remote protocol surface next to CAS and refs. It stores:

- advisory locks,
- the active execution id for a cache key,
- launch state for resumable work,
- execution lifecycle records,
- caller/callee lineage edges,
- invalidation and cancellation tombstones,
- adapter IO objects under `io/...`.

This is what makes async and detached execution workable across processes. The local repo still owns typed DAG state, but the remote side owns the coordination data needed to produce or invalidate that DAG state safely.

At the adapter boundary, the runtime publishes and reloads JSON transport blobs rather than passing Python objects directly. Those payloads carry the runnable, argv pointer, cache key, execution id, remote config, execution lifecycle status, and resume state needed for later polls. Adapters answer with a similarly small status payload so the runtime can decide whether to keep waiting, import a successful DAG, or materialize a failure.

## Remote cleanup

Remote cleanup is split in two on purpose:

- `prune()` removes old transport blobs under `io/invoke/**`, which are ephemeral.
- `gc()` performs mark-and-sweep over CAS objects using published refs as roots.

That distinction matters because transport blobs are not part of the durable repository graph, while manifests and CAS objects are.

For the public-facing model of remotes, also see `docs/concepts/remotes.md` once the concepts lane is rebuilt.
