---
status: specified
doc_type: spec
---

# Contrib Status

## Authority

This document is authoritative for the structured status/introspection contract exposed by `daggerml.contrib.status`.

This document owns:

- the `daggerml.contrib.status.status()` report shape,
- status discovery and introspection behavior for contrib adapters, executors, and codecs,
- status diagnostics behavior,
- JSON-serialization requirements for the returned report.

This document does not own adapter attribute schemas, executor attribute schemas, codec object schemas, registry lookup semantics, or CLI text formatting.

## Scope

In scope:

- Python status API behavior in `daggerml.contrib.status`,
- effective registration reporting for loaded adapter, executor, and codec objects,
- object identity and interface-summary projection rules for reported objects,
- diagnostics produced while building the status report.

Out of scope:

- normative meaning of adapter or executor attributes,
- normative meaning of codec object attributes beyond the codec interface,
- adapter/executor runtime behavior,
- CLI rendering or terminal formatting.

## Purpose

Provide one deterministic, JSON-safe status report that answers what contrib plugins are effectively available, what they point to, and what failed during discovery or introspection.

## Glossary

- Status Report: the dictionary returned by `daggerml.contrib.status.status()`.
- Registration Record: a record describing one loaded adapter, executor, or codec object.
- Effective Registration: a Registration Record representing the object that runtime lookup/iteration would use at the time the Status Report is produced.
- Diagnostic Record: a record describing one non-fatal problem encountered while building the Status Report.
- Reserved Field: a field defined normatively by this document.
- Adapter Spec: adapter object shape owned by [registries.md](registries.md) and runtime surface requirements owned by [runtime-contract.md](runtime-contract.md).
- Executor Spec: executor object shape owned by [registries.md](registries.md) and runtime surface requirements owned by [runtime-contract.md](runtime-contract.md).
- Literal Codec: codec object interface and ordering rules owned by [../codec-system.md](../codec-system.md).

## Contract

### Interfaces

- Location:
  - `daggerml.contrib.status`
- Required interface:
  - `status() -> dict[str, object]`

`status()` MUST return a Status Report that is safe to serialize as JSON without custom encoders.

`status()` MUST return a dictionary with exactly these top-level Reserved Fields:

- `schema_version`: integer; MUST be `1`.
- `summary`: dictionary.
- `adapters`: list of Registration Records.
- `executors`: list of Registration Records.
- `codecs`: list of Registration Records.
- `diagnostics`: list of Diagnostic Records.

Top-level field handling:

- `status()` has no user inputs.
- Constraints: Consumers MUST treat the top-level Status Report shape for `schema_version == 1` as closed except where this document explicitly marks a nested mapping as open.
- Errors and failure modes: internal status assembly failure is raised.
- Invocation surfaces: python API call.

`summary` MUST be a dictionary with exactly these Reserved Fields:

- `has_errors`: `bool`.
- `diagnostic_count`: non-negative integer.
- `adapter_registration_count`: non-negative integer.
- `adapter_effective_count`: non-negative integer.
- `executor_registration_count`: non-negative integer.
- `executor_effective_count`: non-negative integer.
- `codec_registration_count`: non-negative integer.
- `codec_effective_count`: non-negative integer.

`adapters`, `executors`, and `codecs` MUST each be a list of Registration Records.

Registration Record schema:

- `key`: string.
- `fqn`: non-empty string.
- `effective`: `bool`.
- `implements`: dictionary.

Registration Record rules:

- `key` MUST be:
  - adapter name for records in `adapters`,
  - `{adapter}:{name}` for records in `executors`,
  - `{priority}:{order}:{type_qualname}` for records in `codecs`.
- `fqn` MUST identify the reported object as `{module}:{qualname}` when object-level module and qualname are available.
- `fqn` MUST identify the reported object as `{type_module}:{type_qualname}` when object-level module and qualname are not available and runtime type metadata is available.
- `fqn` MUST be stable for a fixed object identity in a fixed process state.
- `effective == true` means the represented object is the Effective Registration at snapshot time.
- `effective == false` means the represented object was observed during report construction but is not the Effective Registration.

`implements` schema and rules:

- For records in `adapters`, `implements` MUST contain exactly:
  - `resolve_runnable`: `bool`,
  - `send`: `bool`,
  - `cli`: `bool`.
- For records in `executors`, `implements` MUST contain exactly:
  - `resolve_runnable`: `bool`,
  - `start`: `bool`,
  - `poll`: `bool`,
  - `cleanup`: `bool`.
- For records in `codecs`, `implements` MUST contain exactly:
  - `can_encode`: `bool`,
  - `encode`: `bool`.
- Each `implements` value MUST report only presence/callability of the named interface member; it MUST NOT redefine that member's semantics.

Registration collection rules:

- Adapter Registration Records MUST be collected for every adapter object observed while loading adapter entry points and for every adapter object registered at runtime without an entry point.
- Executor Registration Records MUST be collected for every executor object observed while loading executor entry points and for every executor object registered at runtime without an entry point.
- Codec Registration Records MUST be collected for every Literal Codec currently registered in runtime selection order, including codecs registered without an entry point.
- `codecs` MUST be ordered exactly as codec selection order would evaluate them at snapshot time.
- `codec_effective_count` MUST equal `len(codecs)`.
- `adapter_effective_count` MUST equal the number of adapter Registration Records with `effective == true`.
- `executor_effective_count` MUST equal the number of executor Registration Records with `effective == true`.

Diagnostic Record schema:

- `severity`: one of `warning`, `error`.
- `scope`: one of `status`, `adapter`, `executor`, `codec`.
- `code`: non-empty string.
- `message`: non-empty string.
- `source`: dictionary with exactly these Reserved Fields:
  - `kind`: one of `entry_point`, `runtime`, `none`.
  - `group`: `null` or non-empty string.
  - `name`: `null` or non-empty string.
  - `value`: `null` or non-empty string.
- `key`: `null` or non-empty string.

Diagnostic rules:

- `diagnostics` MUST be sorted by `(scope, code, message)`.
- `summary.diagnostic_count` MUST equal `len(diagnostics)`.
- `summary.has_errors` MUST be `true` if and only if at least one Diagnostic Record has `severity == "error"`.
- `status()` MUST report non-fatal discovery, load, validation, and duplicate failures as Diagnostic Records instead of raising.
- Required `code` values are:
  - `entry_point_load_failed`,
  - `registration_invalid`,
  - `duplicate_key`,
  - `introspection_failed`.
- Additional diagnostic codes MUST NOT be added in `schema_version == 1`.

Best-effort behavior:

- `status()` MUST construct the Status Report on a best-effort basis and continue report construction where the underlying registry/codec loaders permit partial continuation.
- A failed entry point load MUST produce:
  - a Diagnostic Record with `code = "entry_point_load_failed"`.
- A loaded object that does not satisfy the relevant registry or codec interface checks MUST:
  - produce a Registration Record only if an object instance was available for introspection,
  - produce a Diagnostic Record with `code = "registration_invalid"`,
  - MUST NOT be marked effective unless runtime lookup/iteration would still use it.
- If two or more Registration Records share the same adapter or executor lookup key, `status()` MUST:
  - mark at most one of them effective,
  - emit at least one Diagnostic Record with `code = "duplicate_key"` for that key.

### Invariants

- The Status Report MUST contain only dictionaries, lists, strings, integers, floats, booleans, and `null`.
- The Status Report MUST be deterministic for a fixed process state and a fixed installed entry-point set.
- Reserved Fields defined by this document MUST always be present.
- `implements` MUST report presence/callability only; it MUST NOT claim behavioral correctness.
- `effective == true` MUST correspond to exactly what runtime lookup or iteration would use immediately after the snapshot is taken.
- `effective == false` MUST NOT be used for any Registration Record that runtime lookup or iteration would use immediately after the snapshot is taken.

### Error Semantics

For each error class, specify:

- retryable or non-retryable,
- transient vs terminal,
- required caller behavior,
- required operator action (if any).

- Entry-point load failure:
  - terminal for that entry point,
  - non-terminal for the Status Report,
  - retryability: non-retryable,
  - required caller behavior: inspect `diagnostics` and continue,
  - required operator action (if any): fix or remove the broken plugin package or entry point.
- Invalid registration or codec object:
  - terminal for that object as a valid registration,
  - non-terminal for the Status Report,
  - retryability: non-retryable,
  - required caller behavior: treat the record as informational only unless it is marked effective,
  - required operator action (if any): correct the plugin object to satisfy its authoritative contract.
- Internal status assembly failure:
  - terminal for the `status()` call,
  - retryability: non-retryable,
  - transient vs terminal: terminal,
  - required caller behavior: surface the exception,
  - required operator action (if any): treat as an implementation defect in `daggerml.contrib.status`.

### Security Boundaries

None identified in this spec.

### Observability

- `status()` is the canonical structured observability surface for contrib plugin discovery and effective registrations.
- `summary` counts MUST be sufficient for callers to detect duplicate and failed plugin loads without parsing free-form text.
- `diagnostics` MUST carry enough source metadata to identify the broken entry point or runtime registration when that source identity is available from the failing path, even though registration records expose only `fqn` instead of raw source/object subrecords.
- Human-readable CLI or terminal rendering MAY be derived from the Status Report, but text formatting is out of scope for this document.

### Authority Handoffs

- Adapter and executor object-shape requirements are authoritative in [registries.md](registries.md).
- Adapter and executor runtime-required callable semantics are authoritative in [runtime-contract.md](runtime-contract.md).
- Literal Codec interface, plugin discovery group, and runtime ordering are authoritative in [../codec-system.md](../codec-system.md).
- CLI invocation surfaces and human-readable formatting are not defined here; this document only defines the structured Status Report consumed by those surfaces.

## Compatibility

- `schema_version` MUST be `1` for this contract.
- Any change to top-level Reserved Fields, `summary` Reserved Fields, Registration Record Reserved Fields, Diagnostic Record Reserved Fields, required `implements` keys, or allowed diagnostic `code` values MUST require a new `schema_version`.
- Changes to adapter attributes, executor attributes, or codec attributes do not require a new `schema_version` unless they change Reserved Fields owned by this document.
- The order of `codecs` is compatibility-relevant because it mirrors runtime codec selection order.

## References

- [README.md](README.md)
- [registries.md](registries.md)
- [runtime-contract.md](runtime-contract.md)
- [../codec-system.md](../codec-system.md)
- [../default-dml-runtime.md](../default-dml-runtime.md)
