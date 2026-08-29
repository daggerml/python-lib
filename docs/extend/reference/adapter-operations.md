# Adapter Operations

`AdapterBase.send(**payload)` receives one of three exact request shapes.

## Invoke

```python
{
    "operation": "invoke",
    "cache_key": str,
    "execution_id": str,
    "remote": {"root": str},
    "runnable": dict,
    "adapter_state": dict | None,
    "scratch_uri": str,
}
```

Return `success`, `retry`, or another nonempty failure code:

```python
{"status": "retry", "adapter_state": dict, "retry_after_ms": 1000, "error": None}
{"status": "success", "adapter_state": dict | None, "error": None}
{"status": "provider-error", "adapter_state": dict | None, "error": "diagnostic"}
```

Retry requires object `adapter_state`; `retry_after_ms` is an optional
nonnegative hint used to set shared `driver.not_before`. Invoke success is valid
only after the runtime has published a result, otherwise the caller records a
protocol-error DAG.

Adapter and executor implementations can validate operation responses with the
canonical public facade entry point:

```python
from daggerml._core import validate_adapter_response
```

Do not import the `_core` implementation module that defines it.

## Cleanup

```python
{
    "operation": "cleanup",
    "cache_key": str,
    "execution_id": str,
    "remote": {"root": str},
    "runnable": dict,
    "adapter_state": dict | None,
    "scratch_uri": str,
    "result_ref": str,
}
```

Cleanup without a typed DAG result is rejected. Success records cleanup
complete, retry leaves it pending and persists continuation/backpressure, and a
failure code records diagnostics. No cleanup outcome changes lifecycle or the
published result. The runtime gives required, eligible cleanup one call before
returning either a freshly established or cached terminal result; it does not
wait synchronously for cleanup retries.

## Cancel

```python
{
    "operation": "cancel",
    "cache_key": str,
    "execution_id": str,
    "argv_ref": str,
    "remote": {"root": str},
    "runnable": dict,
    "adapter_state": dict | None,
    "scratch_uri": str,
    "requested_by": str | None,
}
```

Malformed protocol output raises an adapter protocol error; invoke failure
codes are committed as cached error DAGs. Cancel returns `cancelled`, `retry`,
or an error status. Retry requires object adapter state and may include
`retry_after_ms`; the runtime persists both state and the shared deadline.
Before this call, Phase 1 has selected the execution as `cancel-pending`.
Only `cancelled` permits the runtime-owned CAS transition to `canceled`.

`AdapterBase.cli()` supports `-i`/`-o` as `-`, local paths, or S3 URIs. Its
`--poll` loop repeats `operation="invoke"` while it returns `retry`, then drives
nested cleanup to success or terminal failure before an ephemeral Docker or
Batch environment exits. It never sends `operation="poll"`, which is unsupported.
