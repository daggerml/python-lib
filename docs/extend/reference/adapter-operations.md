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
published result.

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
codes are committed as cached error DAGs. Cancel returns
`cancelled` or an error status. The runtime treats a status other than
`cancelled` as inactive for cancellation coordination. Before this call, Phase
1 has already selected the execution as `cancel-pending` and blocked further
mutation. After any well-formed response, the runtime owns the CAS transition to
`canceled`; the adapter does not persist lifecycle state. Cancellation
confirmation does not prove that no backend work continues.

`AdapterBase.cli()` supports `-i`/`-o` as `-`, local paths, or S3 URIs. Its
`--poll` loop repeats `operation="invoke"` while it returns `retry`, then drives
nested cleanup to success or terminal failure before an ephemeral Docker or
Batch environment exits. It never sends `operation="poll"`, which is unsupported.
