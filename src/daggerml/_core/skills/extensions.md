---
name: daggerml-extensions
description: Build and test DaggerML adapters, executors, codecs, and integration plugins.
---

# DaggerML Extensions

## Choose The Extension Point

Use an adapter for a transport boundary. It lowers delayed runnables through an
executor and carries one JSON-compatible `invoke`, `cleanup`, or `cancel`
request and response. Put backend launch, status, and teardown in an executor.
Use a codec only to normalize Python values during DAG staging; codecs do not
execute work.

The runtime owns execution records, cache pointers, locks, retries, shared
deadlines, result publication, lifecycle transitions, and cancellation
coordination. Extensions must not mutate runtime execution objects or cache
pointers.

## Lower A Runnable

An executor's `resolve_runnable(uri, kwargs, sub)` validates extension-specific
configuration and returns a concrete `Runnable`. Wrapper executors receive a
nested `sub`; leaf executors perform the innermost work. The runnable's adapter
executable is the runtime's transport target.

## Handle Execution Operations

`ExecutorBase.handle()` routes an `invoke` with no `adapter_state` to `start()`
and a later invoke with durable state to `poll()`. `poll` is never a wire
operation. It routes `cleanup` and `cancel` independently of state. Executor
instances hold no live process state, and every operation must be idempotent for
its execution ID.

Return `success`, `retry`, or a nonempty failure status with diagnostics. Retry
requires object `adapter_state` and may include nonnegative `retry_after_ms`.
Validate responses with `daggerml._core.validate_adapter_response`, imported
from the `_core` facade.

```python
{"status": "success", "error": None, "adapter_state": None}
{"status": "retry", "error": None, "adapter_state": {"job": "123"}, "retry_after_ms": 1000}
{"status": "provider-error", "error": "diagnostic", "adapter_state": None}
```

## Clean Up Or Cancel Work

Cleanup runs after result publication and receives a typed `result_ref`. It must
tolerate repetition and cannot publish, invalidate, or alter the result or
lifecycle. Cancellation receives `argv_ref` and `requested_by`; return
`cancelled` only after teardown completes.

A wrapper must preserve identifiers, durable state, remote and scratch context,
cleanup result context, and cancellation fields when forwarding nested work.

## Write A Codec

A `LiteralCodec` implements narrow, deterministic `can_encode(value)` and
`encode(value, dag)`. Encoding must make recursive progress toward durable
DaggerML literals, collections, references, `Uri`, or `Runnable`; returning the
same input type fails normalization.

## Register Plugins

```toml
[project.entry-points."daggerml.contrib.adapters"]
site = "site_dml.adapter:SiteAdapter"

[project.entry-points."daggerml.contrib.executors"]
site = "site_dml.executor:SiteExecutor"

[project.entry-points."daggerml.codecs"]
site = "site_dml.codecs:literal_codecs"
```

Adapter and executor entry points load classes directly. A `daggerml.codecs`
entry point loads a zero-argument factory returning `(priority, codec)` pairs.
Installation registers plugins; importing their module does not. Adapter and
executor names must match the delayed runnable's `adapter` and `uri`.

## Test An Extension

Test lowering and exact runnable output, operation payloads, start/poll routing,
durable retries, repeated cleanup and cancellation, malformed responses, nested
forwarding, and isolated plugin discovery before infrastructure tests.

For script-backed funks, remember that workers receive rendered function and
injected source rather than module globals. Import inside the function or supply
dependencies through `extra_objs` or `post_lines`.
`daggerml.contrib.testing.defunkify()` can exercise an innermost retained script
callable, but it does not emulate the runtime lifecycle.
