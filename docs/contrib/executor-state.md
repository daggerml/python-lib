# Executor State

## Status

specified

## Authority

This document is authoritative for contrib executor-state record shape and backend reference conventions.

Normative lifecycle ownership and parent-comms behavior are authoritative in [runtime-contract.md](runtime-contract.md); live execution-graph storage, caller edges, and cancel or sweep behavior are authoritative in [execution-graph.md](execution-graph.md); this document is authoritative for the shared State record/reference shape used by those contracts.

## Purpose

Provide a focused state-reference profile for contrib runtime planning and implementation.

## Scope

This document defines:

- shared execution-state record fields,
- backend profiles (`LocalState`, `DynamoState`),
- ownership and metadata conventions for executor-managed state,
- how parent comms reuses State backends without redefining a second mutable record format.

This document does not define kickoff or poll dispatch rules, live execution-graph tables, or cancel propagation.

## Reference State Record

Common fields used by contrib execution state:

- `version`
- `cache_key`
- `status`
- `error` (nullable)
- `heartbeat_ts`
- `metadata` (`dict[str, dict[str, Any]]`), namespaced by executor id

Reference record example:

```json
{
  "version": 1,
  "cache_key": "abc123",
  "status": "pending",
  "error": null,
  "heartbeat_ts": 1710370000.123,
  "metadata": {}
}
```

State ownership and metadata conventions:

- executors/wrappers MUST write custom state only under `metadata[<executor_id>]`.
- `status`/`error` are canonical run result fields and MUST remain normalized across wrappers.
- Parent Comms reuses these same State backends and record fields for observational reporting.
- Parent Comms is immutable invocation input naming where the current invocation reports outward; it is not a second mutable record schema.
- when adapters report to Parent Comms, they mirror normalized status/heartbeat information into another State record keyed for the parent observer.
- Parent Comms is one-hop only; it applies to the current adapter invocation and is not forwarded to grandchildren.
- executor-specific external handles used for debugging/polling (for example docker container ids or batch job ids) belong under `metadata[<executor_id>]` in the relevant State record.
- `heartbeat_ts` is updated on every state mutation; stale detection uses `heartbeat_ts + HEARTBEAT_STALENESS < time.time()`.

Typed state API surface:

```python
Status = Literal["pending", "running", "succeeded", "failed", "canceled"]

def init_record(
    *,
    status: Status = "pending",
    error: str | None = None,
    metadata: dict[str, dict[str, Any]] | None = None,
) -> StateRecord: ...

def update_status(
    *,
    status: Status,
    error: str | None = None,
) -> StateRecord: ...

def set_executor_metadata(self, executor_id: str, data: dict[str, Any]) -> StateRecord: ...
```

State backends (`LocalState`, `DynamoState`) are generic storage/locking interfaces.
Runtime interpretation of these fields is owned by runtime orchestration, adapters, and executors rather than by backend-specific serialization code.

Lock contract:

- state backends expose lock acquisition as a contextmanager (`State.lock(cache_key)`), yielding locked state instance or `None` when lock acquisition fails.
- lock release is automatic on context exit.

## Backend Profiles

- `LocalState`: process-local backend profile for local adapter or executor flows.
- `DynamoState`: cross-invocation backend profile for lambda-style polling flows.

This backend profile list does not by itself define the live execution-graph storage backend for a contrib runtime deployment.

## Parent Comms Reuse

- Parent Comms MAY point at any supported State backend profile.
- A local Parent Comms descriptor identifies a local state location such as `cache_dir`.
- A Dynamo Parent Comms descriptor identifies backend coordinates such as `table_name`.
- The parent observer reads an ordinary State record from that backend; there is no separate comms-specific mutable file/document schema.

## References

- [runtime-contract.md](runtime-contract.md)
- [execution-graph.md](execution-graph.md)
