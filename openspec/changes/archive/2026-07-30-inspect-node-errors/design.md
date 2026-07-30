## Context

A failed function DAG stores its terminal failure as an `error:*` ref. Today `FnNode.datum_ref()` dereferences and raises that object, so creating an API `Node` for a failed call raises before callers can identify the failed node or its function-DAG context. The CLI can identify refs through DAG descriptions but cannot dereference an error ref through a public query.

This change is intentionally narrow. It preserves the existing `start_fn() -> Ref | None` contract, the current generated CLI mechanism, and the rule that errors are terminal DAG state rather than values.

## Goals / Non-Goals

**Goals:**
- Preserve datum and error references through core node resolution without loading stored errors.
- Provide public low-level inspection of failed node values and explicit error refs.
- Preserve high-level fail-fast node access while attaching the failed node's provenance to the raised error.
- Ensure transient API error subclasses cannot be persisted with non-model fields.

**Non-Goals:**
- Do not add an `ErrorNode`, a result wrapper, or a general resolution helper.
- Do not support errors as literal values, collection members, function arguments, or function results.
- Do not create a function DAG, cache key, or execution for a call whose input resolves to an error.
- Do not alter CLI generation or DML error serialization formats.

## Decisions

### Resolve nodes to datum-or-error refs

`Node.datum_ref()` will return `(datum_ref, error_ref)`, with exactly one non-`None` ref. Literal and argv nodes return their datum ref; imports and successful function nodes propagate their selected node's pair; a failed function node returns its child DAG's `error` ref.

This keeps the persistent graph ref-based and avoids using a stored failure as internal exception control flow. Returning a hydrated `Error` from `datum_ref()` was rejected because it needlessly dereferences persistent objects before the caller chooses inspection or consumption behavior.

### Materialize only at the consuming boundary

`dml.dag.get_node()` will load the selected datum value on success or load and return the stored `Error` on failure. A new `dml.dag.get_error(error_ref)` query will validate and load a specific error ref.

Consumers that require a usable datum, including function invocation, builtin execution, and cache-key computation, will load and raise an error ref before continuing. A failed input is an invalid attempted execution; `start_fn()` will not create a call node or return an error result.

### Raise a transient API error for high-level access

`api.NodeError` will subclass `Error` and retain the failed `node_ref` plus its source `Dag`, which is needed to inspect provenance. `_make_node()` and `Node.value()` will translate an `Error` returned by the low-level query into `NodeError`.

`NodeError.context()` will use structural node descriptions, following import references as necessary, to return the function DAG that recorded the failure. It will not materialize nodes or reuse success-only result provenance traversal.

Returning an inspectable `ErrorNode` was rejected because it changes normal lookup semantics and conflicts with the distinction between a node reference and an exception. Multiple inheritance from `Error` and `Node` also conflicts with their dataclass and persistence behavior.

### Canonicalize errors only at transaction storage

`Error.from_ex()` will copy any `Error` instance or subclass into a new exact base `Error` containing only `message`, `origin`, `type`, and `stack`. `TxnWithValid.put()` will perform this conversion for any error object before validation and serialization.

This one storage boundary ensures a caught `NodeError` cannot persist its transient node or DAG fields. No additional normalization is added in API or index commit methods.

### Retain existing unsupported-literal behavior

No error insertion branch is added. Errors have no datum encoding and remain unsupported as literals. The API codec staging path will not treat `Error` as an accepted literal value; lower-level literal insertion continues to fail normal datum validation for unsupported objects.

## Risks / Trade-offs

- [Every `datum_ref()` consumer must handle a two-ref result] → Update the small set of existing call sites and add tests for both outcomes.
- [A caller can supply a failed node ref through low-level APIs] → Invocation boundaries resolve inputs and raise before argv construction, cache-key creation, or dispatch.
- [`NodeError` subclasses persistable `Error`] → Normalize only in `TxnWithValid.put()` to an exact base `Error`.
- [Failed provenance does not have a result node] → Give `NodeError.context()` a failure-specific structural traversal rather than reusing `Node.context()`.
