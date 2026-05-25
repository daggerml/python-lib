# Consistency Review

This review covers internal consistency and clarity for the active change proposal in `openspec/changes/synchronous-runtime-cancel/`.

## Findings

### 1. Cancellation engine ownership is not stated cleanly

Files:
- `design.md:7-8`
- `design.md:22-25`
- `tasks.md:3`

The design says both:

- `Dml.runtime.cancel` is the single cancellation engine.
- cancellation-specific logic moves into `IndexOps.cancel`.

Then it says `Dml.runtime.cancel` calls `IndexOps.cancel`.

That leaves two plausible interpretations:

- `Dml.runtime.cancel` is the real engine and `IndexOps.cancel` is an implementation helper.
- `Dml.runtime.cancel` is only a thin public entrypoint and `IndexOps.cancel` is the real engine.

`tasks.md` leans toward the second reading, but the design should say that directly.

### 2. Proposal claim about record mutation conflicts with child-worker behavior

Files:
- `proposal.md:9`
- `design.md:78-84`

The proposal says the runtime mutates only the cancelled execution's record during that call.

But the design says a parent cancellation call removes child edges and invokes each child's adapter cancel path directly. That creates ambiguity about whether child execution records are also mutated as part of the parent call.

Possible readings:

- yes, child records are mutated during the parent-driven cancellation flow
- no, only the parent record is mutated and child cancellation is adapter-local

Those models have different implementation and test implications.

### 3. Nested cancellation mechanism is underspecified

Files:
- `proposal.md:13`
- `design.md:59-63`
- `design.md:78-84`

The proposal says adapters may call `Dml.runtime.cancel` once for nested work.

But the design's worker algorithm says the parent worker invokes the child's adapter cancel path once. Those are different mechanisms:

- recursive runtime-level cancellation via `Dml.runtime.cancel(child)`
- direct adapter-level child cancellation from the parent flow

The change should pick one model or define when each applies.

### 4. "Synchronous cancellation" does not yet define the completion bar precisely

Files:
- `proposal.md:7`
- `design.md:37-45`
- `design.md:76-84`

The change is framed as one synchronous cancellation flow, but the design also says adapter cancel return values are ignored for lifecycle purposes.

That leaves the completion rule unclear:

- does `cancelled` mean all runtime-owned work is complete
- does it mean the adapter acknowledged the cancel request
- does it mean external infrastructure is fully torn down

Right now the most likely reading is "wait for the local cancel handlers to return, then mark `cancelled`," but that is not stated explicitly.

### 5. `cancellation_requested_by` provenance is not defined tightly enough

Files:
- `design.md:65-73`
- `tasks.md:23`

The design says `cancellation_requested_by` may contain either a user identity or an execution id.

What remains unclear is which execution id should be recorded in nested cases:

- the immediate parent execution
- the top-level cancellation root
- the adapter/executor currently propagating the request

That choice affects traceability and expected test assertions.

### 6. "May recurse once" is too vague to implement consistently

Files:
- `proposal.md:13`
- `design.md:59-63`
- `design.md:92`
- `tasks.md:17`

The rule says an adapter cancel handler may call `Dml.runtime.cancel` for nested work exactly once in its chain.

"In its chain" is ambiguous. It could mean:

- one call per adapter layer
- one call per execution
- one call per end-to-end adapter stack
- one call per cancelled subtree

The risks section already notes duplicate recursion if ownership is unclear, which is a sign the rule still needs a sharper contract.

### 7. Edge semantics mix historical and live interpretations

Files:
- `design.md:86-88`
- `tasks.md:24`

The design says edges still mean "caller once depended on callee," which sounds historical, but also says cancellation removes those edge objects mechanically.

If cancelled edges are deleted, then persisted edge records are no longer a full history of past dependency facts. The intended rule seems closer to:

- edges are live caller relationships
- they may also be observed as history only until cancellation removes them

That should be stated directly to avoid mismatched expectations in implementation and tests.
