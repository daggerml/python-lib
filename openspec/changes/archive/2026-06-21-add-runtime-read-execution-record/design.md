## Context

The shared `Dml.runtime` namespace already exposes read-oriented runtime inspection methods such as `describe`, `list`, `describe_graph`, and `cancel`, while the underlying execution-state layer already knows how to read one stored execution record from the remote `exec/state/<execution_id>.json` object. The missing piece is a direct caller-facing method that bridges those layers without inventing a new payload shape.

The requested API shape is narrow: `dml.runtime.read_execution_record()` accepts `Ref | str`, resolves that input to an execution id, and returns the raw execution record typed dict. The method is intentionally inspection-only and should preserve the existing `remote.root` requirement and missing-record failure semantics from the execution-state layer.

## Goals / Non-Goals

**Goals:**

- Expose a read-only `Dml.runtime` method for fetching one execution record.
- Support both runtime-index refs and plain execution-id strings at the shared API boundary.
- Return the execution record exactly as stored, without reshaping, filtering, or enrichment.
- Reuse existing execution-state reading logic and error behavior.

**Non-Goals:**

- Changing the execution-record schema.
- Adding search, listing, or graph-expansion behavior beyond a single-record read.
- Introducing alternate transport/storage backends or local mirrors for execution records.
- Reworking `ExecutionState.read_execution_record(...)` to accept `Ref` directly.

## Decisions

### Expose the reader on `Dml.runtime`

The new method belongs on `Dml.runtime` because callers already use that namespace for runtime inspection and control-plane workflows. This keeps execution-record access alongside `describe_graph(...)` and `cancel(...)` instead of forcing callers down into lower-level subsystem objects.

Alternative considered:

- Expose the reader only through `dml.ops` or `ExecutionState`: rejected because it would bypass the existing shared-`Dml` inspection surface and would not satisfy the user-facing API goal.

### Normalize `Ref | str` at the namespace boundary

`Dml.runtime.read_execution_record(execution: Ref | str)` should accept `Ref | str` and normalize either form to the execution id string before delegation. This mirrors the existing ergonomics of `describe_graph(...)`, lets callers pass runtime index refs directly, and keeps lower-level storage readers focused on exact execution-id lookups.

Alternatives considered:

- Accept only `str`: simpler, but it makes callers manually unwrap `Ref("index:<id>")` values even though the shared runtime namespace already has precedent for doing that normalization itself.
- Teach `ExecutionState.read_execution_record(...)` to accept `Ref | str`: rejected because `ExecutionState` is the lower-level storage reader and should keep an exact storage-oriented input contract.

### Return the raw typed-dict payload unchanged

The method should return the stored execution record as-is. That preserves the storage contract already owned by `runtime-execution-records`, avoids duplicating fields into a second API-specific shape, and keeps this method honest as a direct state read rather than a synthesized summary.

Alternatives considered:

- Return a reshaped summary payload: rejected because it would create a second execution-status schema and drift risk with the stored record.
- Enrich the payload with derived graph or cache data: rejected because that turns a direct read into a multi-source inspection workflow.

## Risks / Trade-offs

- [Raw payload exposes storage-shaped field names] -> Accept this explicitly in the contract so callers understand they are reading runtime-owned state, not a polished summary object.
- [`Ref | str` can blur index ids and execution ids] -> Normalize only to the id string and continue delegating to the existing single-record reader, which preserves the exact missing-record error when no execution record exists for that id.
- [Publicly exposing execution records can invite callers to depend on incidental details] -> Scope the method contract to the existing typed-dict schema already owned by the execution-record capability rather than undocumented extra metadata.
