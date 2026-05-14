---
status: specified
doc_type: spec
---

# Public API Module (`daggerml.api`)

## Authority

This document is authoritative for the public Python API surface exported by `daggerml.api`, including public wrapper types, public method and attribute semantics, argument normalization at the API boundary, wrapper selection on reads, and user-facing error surfacing for those interfaces.

This document is not authoritative for object-model definitions, DAG persistence semantics, internal staging mechanics, codec contracts, adapter payloads, remote cache layout, or internal error taxonomy.

## Scope

In scope:

- public wrapper objects exposed by `daggerml.api`,
- public `Dag` named-node access behavior,
- public DAG call-entry staging behavior,
- public node-wrapper selection behavior for staged and persisted reads,
- how these interfaces surface deterministic failures to callers.

Out of scope:

- internal ops implementation details,
- object storage formats and object invariants,
- execution-runtime payload formats,
- remote protocol and cache storage layout,
- detailed error taxonomy outside the public API boundary.

## Purpose

Define the stable user-facing contract for DAG authoring, execution entry, and node access through `daggerml.api`.

## Glossary

- `API`: the public application-programming interface covered by this specification.
- `daggerml.api`: the public Python module that exports the wrapper objects and invocation surfaces specified in this document.
- `Dml`: public runtime/session entrypoint exposed by `daggerml.api`.
- `DAG`: directed acyclic graph value; authoritative structural and lifecycle semantics are in [dag-model.md](dag-model.md).
- `Dag`: public API wrapper for a DAG value.
- `Node`: public API wrapper for a staged or persisted node value.
- `Runnable`: public callable value shape; authoritative model definition is in [object-model.md](object-model.md).
- `Ref`: namespace-qualified identity; authoritative definition is in [object-model.md](object-model.md).
- `Uri`: URI-backed value type; authoritative definition is in [object-model.md](object-model.md).
- `RunnableNode`: public `Node` wrapper used when the underlying persisted value is a `Runnable`.
- `ScalarNode`: public `Node` wrapper used when the underlying persisted value is a non-container value other than a `Runnable`.
- `CollectionNode`: public `Node` wrapper used when the underlying persisted value is list-like or dict-like.
- `DmlRepoError`: repository-domain failure surfaced through the API boundary; authoritative taxonomy is in [errors.md](errors.md).
- `IndexOps`: internal staging subsystem; authoritative behavior is in [internal/ops/index-ops.md](internal/ops/index-ops.md).

## Contract

### Interfaces

- Wrapper exports:
  - `daggerml.api` MUST expose public wrapper objects for `Dml`, `Dag`, `Node`, `Ref`, `Uri`, and `Runnable`.
  - The API boundary MUST normalize caller-supplied Python values into the public wrapper and staging surfaces defined by this document before delegating to internal subsystems.
- `Dag` named-node access:
  - Public named-node access MUST be available directly on `Dag` by item access and attribute access.
  - `dag["name"]` MUST be the canonical named-node access surface and MUST always address the DAG name map.
  - `dag.name` MAY resolve to a named node only when `name` does not resolve to an existing `Dag` attribute, property, or method.
  - `dag.result` MUST resolve to the `Dag` result property.
  - `dag["result"]` MUST resolve to the node named `"result"` in the DAG name map.
  - Unresolved names MUST fail deterministically with public API error behavior.
  - A separate proxy surface for named-node lookup is not part of the public contract.
  - Item access accepts only explicit node-name keys for this contract; names outside the DAG name map are rejected rather than ignored or preserved.
- Wrapper materialization on reads:
  - Persisted `Runnable` values MUST materialize as `RunnableNode`.
  - Persisted `Uri` values MUST materialize as `ScalarNode` rather than `RunnableNode`.
  - Persisted non-container values other than `Runnable` MUST materialize as `ScalarNode`.
  - Persisted list-like and dict-like values MUST materialize as `CollectionNode`.
  - No other wrapper remapping is part of the public contract.
- Public DAG-call entry:
  - Invocation surfaces are `Dag.call(*args, **kwargs)` and `RunnableNode.__call__(*args, **kwargs)`.
  - Accepted argument forms are positional call arguments, keyword call arguments, existing public node wrappers, and plain Python values accepted by the public staging surface.
  - Success return shape is a public `Node` wrapper representing the staged function-result node or staged result node produced by the call-entry operation.
  - `Dag.put(...)` and `Dag.call(...)` MUST normalize caller-supplied values through the codec system in `daggerml.codecs` before runtime staging.
  - Public DAG function invocation through `Dag.call(...)` and `RunnableNode.__call__(...)` MUST stage call arguments through DAG nodes before adapter or runtime execution begins.
  - Public DAG function invocation MUST stage the callable itself through the same codec-driven insertion path used for arguments.
  - When a supplied argument is already a node, the API MUST preserve that node identity instead of copying it through a new literal node.
  - Unknown callable keyword arguments surfaced during keyword-argument validation MUST be reported deterministically to callers.
  - Side effects: this interface mutates active DAG staging state by creating or reusing staged nodes needed for the call-entry operation.
  - Invocation-surface constraints: callers MUST use the positional and keyword invocation surface only; there is no separate options envelope or auxiliary control-field map on this interface.
  - Unspecified or extra fields are not preserved across this interface: call inputs are normalized into staged node arguments or rejected with deterministic public API failures.

### Invariants

- Public API wrappers MUST preserve the semantic distinction between runnable values, scalar values, and collection values when materializing `Node` wrappers.
- `Dag` item access and `Dag` attribute access MUST NOT be conflated: item access always targets the DAG name map, while attribute access first reserves existing `Dag` API members.
- The meaning of `dag.result` MUST remain distinct from the meaning of `dag["result"]`.
- Public DAG-call entry MUST materialize all non-node arguments as DAG nodes before execution begins.
- Public API staging MUST preserve identity for caller-supplied node arguments.
- API-owned behavior MAY delegate internal implementation, but delegation MUST NOT change the public semantics defined in this document.

### Error Semantics

- Missing remote-context errors:
  - Applies when non-builtin public DAG-call execution requires remote context that is not configured.
  - Classification: non-retryable until configuration changes; terminal for the current invocation.
  - Caller behavior: provide the required remote configuration, then retry the call.
  - Operator action: ensure the runtime environment provides the configured remote root and cache context when that environment is externally managed.
- Callable argument contract errors:
  - Applies to unknown callable keyword arguments surfaced through public DAG-call entry.
  - Classification: non-retryable for unchanged inputs; terminal.
  - Caller behavior: correct the provided call shape before retrying.
  - Operator action: none required.
- Delegated repository-domain errors:
  - The API MAY surface `DmlRepoError` and related public error values produced by delegated subsystems.
  - Retryability, transient-versus-terminal classification, and taxonomy for those delegated errors are authoritative in [errors.md](errors.md).
  - Caller behavior and operator action for delegated errors follow [errors.md](errors.md).

### Security Boundaries

- `daggerml.api` is a caller-facing boundary that accepts local Python values and delegates non-builtin execution to lower layers that may require remote context.
- The public API MUST NOT redefine or bypass the remote-context requirements imposed by delegated execution and cache subsystems.
- When remote context is required for non-builtin execution, the API boundary MUST require that context to be present before treating the operation as valid.
- This document does not define authentication material, secret transport, or remote protocol trust rules.
- Auth, secret handling, adapter payload trust, and remote transport requirements are authoritative in [adapter-execution-contract.md](adapter-execution-contract.md), [default-dml-runtime.md](default-dml-runtime.md), [remote-data-model.md](remote-data-model.md), and [remote-protocol.md](remote-protocol.md).

### Authority Handoffs

- [object-model.md](object-model.md) is authoritative for the model definitions and invariants of `Ref`, `Uri`, `Runnable`, DAG objects, and persisted node families.
- [dag-model.md](dag-model.md) is authoritative for DAG semantics outside the public wrapper surface defined here.
- [execution-model.md](execution-model.md) is authoritative for end-to-end execution ordering and result-materialization semantics after public call-entry staging.
- [adapter-execution-contract.md](adapter-execution-contract.md) is authoritative for adapter payload shape, cache-key semantics, and adapter success requirements.
- [default-dml-runtime.md](default-dml-runtime.md) is authoritative for default runtime behavior outside the public API surface defined here.
- [errors.md](errors.md) is authoritative for error taxonomy and stable repository-domain error contracts.
- [codec-system.md](codec-system.md) is authoritative for codec contracts.
- [internal/ops/index-ops.md](internal/ops/index-ops.md) is authoritative for internal staging mechanics used to realize public DAG-call entry.

## Compatibility

- Backward compatibility: compatible releases MUST preserve the documented meaning of direct `Dag` named-node access, node-wrapper selection, and DAG-call staging behavior for existing callers.
- Forward compatibility: callers MAY rely only on the interfaces and semantics defined in this document; undocumented attributes, helper methods, and internal delegation details are not forward-compatible surfaces.
- Versioning boundary: a release that changes the precedence, interpretation, required availability, or required error behavior of any interface in `### Interfaces` is a compatibility break for the public `daggerml.api` surface and MUST be accompanied by an intentional versioning boundary and spec update.
- Versioning: additive public API growth is allowed in compatible releases, but new helpers or wrapper methods MUST NOT change the precedence, interpretation, or error behavior of the interfaces specified here.
- Internal implementation changes are allowed when they preserve the public behavior defined here.
- This document makes no compatibility guarantee for internal modules, undocumented helper surfaces, or delegation internals.

## References

- [dag-model.md](dag-model.md)
- [object-model.md](object-model.md)
- [execution-model.md](execution-model.md)
- [adapter-execution-contract.md](adapter-execution-contract.md)
- [default-dml-runtime.md](default-dml-runtime.md)
- [errors.md](errors.md)
- [codec-system.md](codec-system.md)
- [remote-data-model.md](remote-data-model.md)
- [remote-protocol.md](remote-protocol.md)
- [internal/ops/index-ops.md](internal/ops/index-ops.md)
