## Context

Runtime indexes are local mutable commit-shaped objects addressed by generated `index:<id>` refs. Their ID is also the root execution ID used by remote execution records, cancellation, cache invalidation, and execution graphs. The partial DAG is already independently addressable and can be inspected without a terminal result.

## Goals / Non-Goals

**Goals:**
- Persist a frozen representation of a user runtime without changing its execution identity or partial DAG.
- Make frozen runtimes discoverable through existing runtime inspection APIs.
- Keep the generated runtime ref as the durable address and use an optional message only for display-time disambiguation.

**Non-Goals:**
- Named-index filesystem pointers, runtime name lookup, notification delivery, or user-input collection.
- A new execution-record lifecycle value; frozen is local runtime representation, not remote execution lifecycle.
- New node or DAG query endpoints.
- New mutation-gating semantics for frozen runtimes beyond existing runtime validation.

## Decisions

### Represent frozen state with a distinct persistent object

Add `FrozenIndex(Commit)` rather than subclassing `Index`. It carries the commit-shaped fields, the partial `dag` ref, and `frozen_message: str | None`. Its object ref uses a frozen-index namespace, while the object ID remains unchanged.

Freezing replaces `index:<id>` with `frozenindex:<id>` in one local write transaction; unfreezing performs the inverse. Only one representation is retained. The unchanged ID means the existing execution record at `exec/state/<id>.json` and all lineage edges remain valid.

Alternative considered: add a mutable lifecycle field to `Index`. This would conflate local representation state with the remote execution lifecycle, which is the established lifecycle authority.

### Limit freeze to user roots

Freeze verifies that the runtime is a user root, rather than an execution-aware worker index. The check is based on the partial DAG/runtime shape used to distinguish function runtimes (function runtimes have an argv node) and must reject before replacing local state.

This avoids suspending a script executor or adapter worker that needs to return a terminal function DAG to its caller.

### Reuse DAG inspection

`runtime.describe()` returns the partial DAG ref for either representation. `runtime.list()` enumerates both namespaces and includes a state discriminator plus the optional frozen message. Consumers use existing `dml.dag` and node APIs for the partial graph; no frozen-specific read path is introduced.

### Preserve existing execution behavior

Cancellation, invalidation, and graph code work from execution IDs and remote records, not local object namespaces. Their entrypoints will accept either runtime ref and derive the common ID. Local GC must enumerate both local index namespaces as roots.

The change does not add explicit mutation rejection for `FrozenIndex`. Mutation behavior remains governed by existing validation and is outside this change's contract.

## Risks / Trade-offs

- [Namespace transition leaves callers holding an old ref] -> `freeze()` and `unfreeze()` return the replacement ref; callers must retain that returned value.
- [Users cannot assign stable human names] -> runtime list exposes creation metadata and optional freeze messages; a future naming scheme can be added without changing execution identity.
- [GC misses frozen objects] -> add both frozen refs and their partial DAGs to the local root enumeration contract and test it.
- [A worker is frozen accidentally] -> reject non-user runtime indexes before transition.

## Migration Plan

The feature introduces a new local object namespace only. Existing repositories have no frozen objects and retain current behavior. Rollback consists of unfreezing any live frozen runtimes before running a version without the new namespace; compatibility for persisted frozen objects is not provided by older versions.
