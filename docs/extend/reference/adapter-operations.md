# Adapter Operations

`AdapterBase.send(**payload)` receives one of two request shapes.

## Invoke

```python
{
    "operation": "invoke",
    "cache_key": str,
    "execution_id": str,
    "remote": {"root": str},
    "runnable": dict,
    "state": dict | None,
    "scratch_uri": str,
}
```

Return one of:

```python
{"status": "running", "state": dict, "dag_id": None, "error": None}
{"status": "succeeded", "state": None, "dag_id": "<dag id>", "error": None}
{"status": "failed", "state": None, "dag_id": None, "error": "message"}
```

## Cancel

```python
{
    "operation": "cancel",
    "cache_key": str,
    "execution_id": str,
    "argv_ptr": str,
    "remote": {"root": str},
    "runnable": dict,
    "state": dict,
    "scratch_uri": str,
    "requested_by": str | None,
}
```

Return `{"status": "cancelled", "error": None}` after handling the request,
or `{"status": "failed", "error": "message"}`. The runtime treats a status
other than `cancelled` as inactive for cancellation coordination. It may have
already revoked active ownership before this call, so cancellation confirmation
does not prove that no backend work continues.

`AdapterBase.cli()` supports `-i`/`-o` as `-`, local paths, or S3 URIs. Its
`--poll` loop only repeats invoke requests while they return `running`.
