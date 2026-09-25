---
name: daggerml-querying
description: Use when reading or investigating DaggerML results. Applies DML-specific guidance for selecting data, tracing provenance, and interpreting persisted failures.
---

# DaggerML Querying

Treat the DaggerML docs and current source as the authority for API behavior.

Use this skill to decide what to inspect and how to interpret it, not to
replace the CLI or Python API reference.

Start with `dml show` to discover DAGs and `dml dag --help` to find the relevant
inspection command for recorded graph state, provenance, refs, and errors.
Use Python when working interactively with a specific DAG's nodes, nested
values, projections, artifacts, or provenance traversal is more useful than
inspecting recorded metadata through the CLI.

## Establish The Object Of Investigation

Identify the intended DAG and revision before interpreting a result. A name at
`HEAD` may no longer identify the result under investigation; preserve exact
refs for historical results.

A committed DAG is immutable and has a terminal `.result`. An active runtime
can still change; a frozen runtime exposes a stable, read-only partial DAG.
Both partial states expose named nodes and `argv`, but neither has a committed
terminal result. Querying an active DAG does not freeze or commit it.

## Select The Intended Result

Distinguish the committed terminal output from a named intermediate. In Python,
`dag.result` is the terminal result and `dag["name"]` selects a named node.
A node named `"result"` has no special relationship to `.result`.

When using Python, select the narrowest relevant part of a collection before
calling `.value()`; avoid materializing an entire collection to read one item.

Indexing a committed collection yields a read-only `Projection` with a base
node and path, not its own node ref. Indexing in an open DAG instead records an
access node. Do not infer a new persisted node from every selection. A missing
name is distinct from a named call that exists but failed.

## Follow Provenance

Trace recorded provenance rather than inferring origins from values. Distinguish
the immediate producing context from rooted provenance across function or import
boundaries. Collection construction and access are implemented as builtin funks;
their execution DAGs are generally plumbing, not the producer you want to
attribute a selected value to. Context traversal follows through those builtin
layers in either mode.

In Python, `node.context(root=False)` finds the nearest producing function or
import DAG, while `node.context()` follows those boundaries to rooted provenance.
The `root` option controls whether traversal continues past that nearest
non-builtin boundary, not whether builtin collection layers are skipped.
Projections support the same traversal.

Compare both contexts for nested funks; inspect the producing DAG's inputs,
names, and result before attributing a value to a particular worker. Distinct
authoring nodes may share one execution DAG, so compare immediate execution
refs when investigating reuse.

## Treat Errors As Results Of Investigation

Persisted failures are evidence, not missing values. Preserve the error's
origin, type, message, stack, failing node ref, and producing context. In
Python, failed named calls and failed terminal results surface differently;
do not mistake either for an absent value or silently use another result.

Inspect producing inputs and intermediate nodes before attributing cause or
retrying.

## Follow Refs, Don't Guess

Use CLI inspection to follow the recorded DAG-to-node-to-producing-DAG-to-error
chain when only refs are available. Keep namespace-qualified refs intact; do
not infer producing functions, revisions, or failures from materialized values
alone.

## Check The Interpretation

Before concluding:

- Right revision and committed or partial state?
- Terminal result or named intermediate?
- Projection or independently recorded node?
- Immediate vs rooted provenance understood?
- Persisted error details and producing context preserved?
- Conclusion supported by refs/context rather than just the Python value?
