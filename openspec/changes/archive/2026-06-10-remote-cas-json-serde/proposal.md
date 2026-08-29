## Why

The remote CAS still stores opaque DB raw payloads. That couples `remote.py` to `{get,put}_raw`, forces remote GC to spin up a scratch DB just to inspect objects, and makes the remote protocol less coherent than the local typed-object model.

We want the remote CAS to own a proper JSON wire format for persisted objects.

## What Changes

- **BREAKING**: Replace remote CAS DB-raw payloads with canonical JSON produced by a private serde in `src/daggerml/_core/remote.py`.
- **BREAKING**: Remove all remote CAS dependence on `TxnWithValid.get_raw()` and `TxnWithValid.put_raw()`.
- **BREAKING**: Remove the temporary `DmlDB` used only for remote CAS deserialization during remote GC.
- Serialize persisted object payloads with a fully tagged recursive JSON format instead of ad hoc `Ref` markers.
- Deserialize remote CAS blobs using the expected root namespace from the `Ref`, then recompute identity with `txn.put(obj)` and require the resulting ref to match the expected ref.
- Keep this intentionally clean for v0: no backward compatibility, no dual read/write paths, no migration shim, and no schema-version bridge for the old raw format.
- Keep implementation scoped to `src/daggerml/_core/remote.py` unless the work reveals a real spec mismatch elsewhere.

## Capabilities

### Modified Capabilities

- `remote-object-refs`: Remote CAS object encoding, decoding, integrity checks, and GC traversal semantics.

## Impact

- Affected code: `src/daggerml/_core/remote.py`.
- Affected tests: remote roundtrip and remote GC coverage.
- Affected specifications: `openspec/specs/remote-object-refs/spec.md`.
- Compatibility: backward compatibility is intentionally not preserved.
