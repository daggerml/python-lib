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
    "adapter_state": dict | None,
    "scratch_uri": str,
}
```

Return one of:

```python
{"status": "running", "adapter_state": dict, "dag_id": None, "error": None}
{"status": "succeeded", "adapter_state": dict, "dag_id": "<dag id>", "error": None}
```

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

Invoke `running` responses must return object `adapter_state`; later responses
may omit it when prior state remains usable. Cancel responses may omit or return
null state. Malformed protocol output raises an adapter protocol error; reported
non-success invoke outcomes are committed as cached error DAGs. Cancel returns
`cancelled` or an error status. The runtime treats a status other than
`cancelled` as inactive for cancellation coordination. Before this call, Phase
1 has already selected the execution as `cancel-pending` and blocked further
mutation. After any well-formed response, the runtime owns the CAS transition to
`canceled`; the adapter does not persist lifecycle state. Cancellation
confirmation does not prove that no backend work continues.

`AdapterBase.cli()` supports `-i`/`-o` as `-`, local paths, or S3 URIs. Its
`--poll` loop only repeats invoke requests while they return `running`.
