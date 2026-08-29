## Context

The public `Dml.runtime` namespace sits between typed repository/runtime identities and lower-level execution coordination keyed by string IDs. Most mutation methods already require `Ref`, but `create`, `read_launch_state`, `read_execution_record`, `describe_graph`, and `cancel` accept execution-id strings or mixed `Ref | str` inputs. `cancel` additionally recognizes an `index:` prefix and constructs a `Ref` inside `Dml`.

The generated CLI derives conversion from annotations. An exact `Ref` annotation already converts a textual CLI token into `Ref` before invoking `Dml`, whereas `Ref | str` always prefers string transport. Direct Python and generated CLI calls can therefore share strict `Dml` signatures without CLI-specific branches in the runtime namespace.

Lower-level `IndexOps`, `ExecutionState`, adapter messages, and persisted execution records use opaque string execution IDs. Those internal and storage contracts remain appropriate and do not need migration.

## Goals / Non-Goals

**Goals:**

- Make every runtime or execution identity accepted by `Dml.runtime` a `Ref`.
- Remove caller-string parsing and string-to-`Ref` coercion from `Dml.runtime`.
- Preserve string execution IDs below `Dml` by extracting `ref.id()` exactly once at the boundary.
- Keep generated CLI text conversion generic and annotation-driven.
- Make direct Python, public wrapper, contrib, CLI, test, and documentation usage agree on the same boundary.

**Non-Goals:**

- Changing execution-state object keys, adapter envelopes, cache metadata, or persisted execution record fields from strings to refs.
- Changing revision selector grammar or any `Ref | str` parameter whose string side represents a revision expression.
- Requiring a referenced runtime index to exist in the local LMDB before remote execution-state inspection.
- Introducing CLI command-specific parsing or compatibility coercion in `Dml`.
- Redesigning cancellation result payloads beyond making the requested identity consistently a `Ref`.

## Decisions

### 1. `Dml.runtime` uses refs for all execution identity inputs

The affected surface will use these contracts:

- `create(cache_key: str | None = None, execution: Ref | None = None) -> Ref`
- `read_launch_state(execution: Ref) -> dict | None`
- `read_execution_record(execution: Ref) -> ExecutionRecord`
- `describe_graph(*roots: Ref, visual: bool = False) -> ExecutionGraph | None`
- `cancel(index: Ref, *, mode: Literal["full", "drive"] = "full") -> RuntimeCancelSummary`

Renaming `create.execution_id` to `execution` prevents a `Ref`-typed parameter from advertising itself as a raw ID. The existing pairing rule remains: `cache_key` and `execution` are either both supplied or both omitted. `create` passes `execution.id()` to `IndexOps.create`.

Alternative considered: retain `Ref | str` and reject only strings containing `:`. Rejected because it keeps two identity representations and leaves the generated CLI on string transport.

Alternative considered: keep `create` and `read_launch_state` string-based because their lower layers use IDs. Rejected because the requested public rule applies to all runtime identity inputs; lower-layer representation does not define the public `Dml` contract.

### 2. Runtime refs are identity tokens, not mandatory local dereferences

An execution attempt is represented at the `Dml` boundary as `Ref("index:<execution-id>")`. Inspection and cancellation may extract that ID without first loading an `Index` from local LMDB. This supports remote worker and supervisor workflows whose execution record exists remotely while still preserving typed caller intent.

Execution-aware `create` accepts the `index` namespace because it creates or activates an active mutable index. Existing runtime inspection and cancellation behavior continues to accept active `index` and preserved-ID `frozenindex` refs where currently supported. Wrong-namespace refs and non-`Ref` values fail at the `Dml` boundary before lower-level delegation.

Alternative considered: add a new `ExecutionId` wrapper. Rejected because runtime identity is already exposed as `Ref` by `runtime.create`, `runtime.list`, and `runtime.freeze`, and another public identity type would recreate dual representation.

### 3. Conversion direction is one-way at the Dml boundary

`Dml.runtime` may convert `Ref -> str` for lower-level delegation. It never converts `str -> Ref`. This direction applies uniformly to creation, launch-state reads, execution-record reads, graph roots, and cancellation.

This leaves lower-level APIs unchanged:

```text
CLI token ──generic parser──> Ref ──Dml.runtime──> execution_id string
Python caller ──────────────> Ref ──Dml.runtime──> execution_id string
```

Alternative considered: migrate `ExecutionState` to refs. Rejected because execution IDs are remote coordination keys rather than LMDB object references, and changing them would create a storage and protocol migration unrelated to API strictness.

### 4. CLI adaptation remains generic

Changing affected annotations to exact `Ref` makes the existing generated CLI parser construct refs from canonical `index:<id>` or `frozenindex:<id>` tokens. CLI tests and docs will use full ref text rather than bare `.id()` values. No runtime-command special cases will be added to `_cli.py` or `Dml`.

The separate shallow conversion behavior for structured values such as `list[Ref]` is outside this change because none of the newly narrowed identity parameters use JSON collection transport.

### 5. Internal callers establish typed identity before calling Dml

Callers that currently possess only an execution ID, especially the contrib supervisor, will construct the corresponding index ref at their own boundary or use an ID-oriented lower-level abstraction when that is the true dependency. `Dml` will not preserve a string overload for those callers.

Higher-level authoring wrappers that already retain runtime tokens as `Ref` continue passing them directly.

## Risks / Trade-offs

- [Existing Python callers pass bare execution IDs] -> Treat this as an intentional breaking change, migrate repository callers, and add negative contract tests for every affected method.
- [A remote execution ref may not resolve in local LMDB] -> Validate type and namespace without requiring local dereference for execution-state-only operations.
- [Wrong namespace refs could silently target an execution with the same ID] -> Validate `index`/`frozenindex` namespace policy before extracting the ID.
- [Generated CLI previously accepted bare IDs] -> Update examples and tests to use canonical ref text and rely on the existing exact-`Ref` parser.
- [Supervisor code must synthesize typed identity from protocol data] -> Perform that conversion where the adapter envelope is interpreted, not inside `Dml`.
- [Downstream users may rely on the `execution_id` keyword to `runtime.create`] -> Rename it without a compatibility alias so the signature accurately communicates its `Ref` contract.

## Migration Plan

1. Add strict runtime-ref contract tests, including rejection of bare IDs and ref-shaped strings supplied directly to Python methods.
2. Narrow the five affected `Dml.runtime` signatures, add namespace validation, and remove coercion branches.
3. Migrate public and contrib callers to retain, pass, or construct runtime refs before invoking `Dml`.
4. Update generated CLI tests to provide canonical ref tokens and confirm methods receive refs.
5. Update runtime documentation and architecture text to distinguish public refs from lower-level execution IDs.
6. Run focused runtime, CLI, API, and contrib tests followed by the complete test suite.

No persisted-data rollout is required. Rollback consists of restoring the old signatures and caller behavior; no storage written by this change requires conversion.

## Open Questions

None. Revision selectors remain strings by explicit scope decision, and runtime execution identity is represented by `index` refs at the `Dml` boundary.
