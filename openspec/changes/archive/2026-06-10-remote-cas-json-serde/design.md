## Context

The current remote CAS path is:

```text
typed object -> DB raw bytes -> remote CAS
remote CAS -> DB raw bytes -> scratch/local DB -> typed object
```

That is the wrong boundary. The remote protocol should serialize persisted objects directly.

## Goals / Non-Goals

**Goals:**

- Give remote CAS its own canonical JSON object format.
- Keep the format collision-free by tagging all recursive values, not only `Ref`.
- Materialize remote objects by typed decode plus identity recomputation.
- Let remote GC inspect remote CAS objects without creating a temporary DB.
- Keep the v0 break clean: no compatibility code.

**Non-Goals:**

- Do not preserve or read the old DB-raw CAS format.
- Do not add migration or dual-write logic.
- Do not move this serde into a shared module yet.

## Decisions

### Remote CAS Uses A Fully Tagged JSON Serde

Remote CAS blobs will be canonical JSON produced by a private serde in `remote.py`.

The serde recursively tags all values:

- `["scalar", value]`
- `["list", [...]]`
- `["dict", {...}]`
- `["ref", "ns:id"]`

This avoids collisions with plain user-authored lists such as `["ref", "..."]` inside persisted data because plain lists and dicts are also tagged.

### CAS Blobs Carry Payload, Not Redundant Root Type

The expected root namespace comes from the `Ref` being loaded, not from the blob itself.

Decode flow:

```text
expected ref -> expected ns -> class
remote blob -> tagged decode -> payload dict
class.from_dict(payload) -> object
txn.put(object) -> actual ref
assert actual ref == expected ref
```

The remote layer must not force objects into place with `to=expected_ref`.

### Integrity Validation Is Identity Re-computation

Remote import integrity is validated by recomputing the ref from the decoded object and requiring an exact match with the referenced CAS id.

This makes the CAS contract explicit and removes trust in remote payload claims beyond the root ref.

### Remote GC Traverses Decoded Objects Directly

Remote GC should load a CAS blob, decode it to a typed object using the expected ref namespace, collect direct child refs, and continue traversal. It should not create a scratch `DmlDB` only to call `put_raw()`.

### Canonical JSON

The serde should emit canonical JSON with stable ordering and strict float handling.

- `sort_keys=True`
- compact separators
- `allow_nan=False`

## Risks / Trade-offs

- Old remote CAS payloads become unreadable: accepted.
- The remote serde becomes a second tagged JSON format beside `_core/serde.py`: accepted for now because the scope is private to persisted-object transport.

## Open Questions

- None currently.
