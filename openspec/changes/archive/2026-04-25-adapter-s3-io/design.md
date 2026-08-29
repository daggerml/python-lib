## Context

Fire-and-monitor executors launch sub-adapters as detached processes (Docker containers, AWS Batch jobs, etc.) where no stdin/stdout pipe is possible. They need an alternative transport for the adapter input payload (normally stdin) and the adapter output result (normally stdout).

The adapter CLI already supports `-i <path>` and `-o <path>` as alternatives to stdin/stdout, and `_read_input` already handles S3 URIs for input. The gap is:

1. No standard S3 path convention for adapter I/O within the `fn-exec/` namespace.
2. No object that encapsulates derivation and access of those paths.
3. `_write_output` does not support S3 URIs — sub-adapters in remote environments cannot write their result back.
4. The `batch` executor has ad-hoc S3 I/O logic outside `fn-exec/`, using `S3Store.cd("jobs")` with random content-addressed keys, which is non-deterministic from the poller's perspective and unrelated to the execution record structure.
5. The `docker` executor works around the same problem with a local tmpdir volume mount, carrying `workdir` and `output_path` in its state — unnecessary machinery that ties the poller to the local filesystem.

Among current executors, `docker` and `batch` are both migrated in this change. `script` pipes directly via supervisor, `ssh` forwards stdin/stdout over the SSH session, and `cfn` has no sub-adapter.

`ExecutionState` already owns the `fn-exec/` namespace and has the raw S3 primitives needed. It is the correct home for this capability.

## Goals / Non-Goals

**Goals:**

- Define a standard `fn-exec/io/{cache_key}/{exec_id}/{name}/` sub-namespace for adapter I/O.
- Provide `AdapterIO`, a scoped object derived from `ExecutionState`, with `input_uri`, `output_uri`, `write_input()`, and `read_output()`.
- Add S3 write support to `AdapterBase._write_output()` so sub-adapters can honor `-o <s3-uri>`.
- Migrate `docker` executor to use `AdapterIO`, removing `workdir`, `output_path`, and tmpdir machinery from its state and cleanup.
- Migrate `batch` executor to use `AdapterIO`.

**Non-Goals:**

- Modifying `S3Store` (content-addressed artifact store — wrong abstraction here).
- Providing adapter I/O for executors that can pipe stdin/stdout directly (script, ssh).
- GC of `fn-exec/io/` objects (out of scope; same lifecycle as execution records, GC addressed separately).
- Any new executor beyond migrating `docker` and `batch`.

## Decisions

### `AdapterIO` lives in `exec_state.py` alongside `ExecutionState`

`AdapterIO` is a scoped view into `ExecutionState`'s S3 primitives and namespace. It is not executor-specific and not part of the adapter contract itself — it is coordination infrastructure. Co-locating it with `ExecutionState` keeps the namespace ownership clear.

Alternatives considered:
- `adapters.py`: rejected — that module is the sub-adapter side; it should not own S3 coordination paths.
- New `adapter_io.py` module: rejected — unnecessary split for a small, tightly coupled class.

### Path: `fn-exec/io/{cache_key}/{exec_id}/{name}/`

- `cache_key` groups all I/O for a given function execution, consistent with the rest of `fn-exec/`.
- `exec_id` is a UUID assigned per execution attempt, making each run's I/O unique even on retry.
- `name` is `"{adapter}:{executor}"` (e.g. `"lambda:batch"`), scoping I/O to the specific adapter/executor pair within a run. This avoids collisions when multiple adapters are involved in the same execution chain and makes the namespace self-documenting.

Alternatives considered:
- Using `exec_number` instead of `exec_id`: `exec_id` is already threaded through both `start()` and `poll()`, so it is the natural key. `exec_number` requires an extra lookup.
- Flat `fn-exec/io/{exec_id}/`: loses the cache_key grouping, harder to GC or inspect by job.
- Outside `fn-exec/` (e.g. `jobs/`): breaks namespace ownership; `fn-exec/` is the authoritative coordination prefix.

### `output_uri` is derivable, not stored in executor state

Because `AdapterIO` derives its paths deterministically from `(cache_key, exec_id, name)` — all available in both `start()` and `poll()` — the poller can reconstruct the same `AdapterIO` instance and call `read_output()` without needing the URI in the persisted state dict. This removes `input_uri` and `output_uri` from the batch executor's state payload.

### `name` is caller-defined, conventionally `{adapter-shorthand}:{executor}`

`AdapterIO` does not enforce a naming scheme — callers pass whatever `name` string is appropriate for their context. Built-in executors use `"{adapter-shorthand}:{executor-name}"` (e.g. `"local:docker"`, `"lambda:batch"`). This convention is sufficient to avoid collisions within the `fn-exec/io/` namespace and makes paths self-documenting. Future executors should follow the same convention but are not required to.

### Docker image tar tmpdir is ephemeral and cleaned up immediately

`DockerExecutor._prepare_image` downloads an S3 image tar to a temporary directory for `docker load`. With this change, there is no longer a persistent workdir for I/O — the image tar tmpdir is created, used for `docker load`, and removed immediately after. It is not part of executor state.

### `_write_output` S3 support uses a direct boto3 PUT

`_read_input` uses `S3Store().get(uri)` because `S3Store.get` handles URI parsing and works without a prefix context. For output, `S3Store.put()` is content-addressed (key derived from SHA256 of data) and cannot write to a pre-determined URI. Rather than modifying `S3Store`, `_write_output` will parse the S3 URI directly and use a minimal boto3 `put_object` call — consistent with how `ExecutionState` handles its own S3 writes internally.

Alternatives considered:
- Adding `put_at(uri, data)` to `S3Store`: rejected per explicit decision to not modify `S3Store` for this feature.
- Importing `ExecutionState` into `adapters.py` for the write: rejected — the sub-adapter side only needs a raw S3 PUT to a URI it was handed; it has no reason to know about `ExecutionState`.

## Risks / Trade-offs

- **No GC defined for `fn-exec/io/`** → These objects accumulate until a future GC pass is defined. Acceptable short-term; `fn-exec/` cleanup is a separate concern.
- **`batch` and `docker` executor state shapes change** → `batch` loses `input_uri` and `output_uri`; `docker` loses `workdir` and `output_path`. Any in-flight executions at deploy time with those keys in their recorded state will not break `poll()` (it no longer reads them from state), but old-format state will have unused keys. Clean cutover acceptable given no versioning contract on executor state internals.

## Open Questions

None.
