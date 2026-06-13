# DAGs and nodes

In DaggerML, a DAG is the stored record of a computation, not just a planning structure.

## What a DAG contains

The core `Dag` type stores:

- `nodes`: the node refs that make up the graph
- `names`: a mapping from user-facing names to node refs
- `result` or `error`: the terminal outcome
- optional `argv`: the node that captures call inputs for function DAGs

That means a DAG can describe both simple literal work and a function execution with recorded inputs and outputs.

## DAGs are immutable snapshots

The public API lets you work with a mutable handle while building a DAG, but the persisted DAG objects are immutable snapshots. Internally, index operations advance by creating new DAG states rather than editing one in place.

That is why the same repository has two layers of state:

- an index for in-progress work
- DAG refs for finished snapshots

Committing saves the current snapshot into history. Loading a committed DAG gives you a stable object you can inspect or import from, but not mutate.

## Node kinds

The stored node types map to a small set of roles:

- `LiteralNode`: a literal value or error payload
- `ImportNode`: a node imported from another DAG
- `FnNode`: the result of calling a runnable with specific arguments
- `ArgvNode`: the positional call inputs for a function DAG
- `KwargvNode`: the keyword call inputs for a function DAG

Most user code does not construct those classes directly. Instead, they appear as the result of assigning Python values, importing nodes across DAGs, or calling runnables.

## Names are labels, not ownership

`Dag.names` is a mapping from strings to node refs. A name points at a node; it does not create a second copy of the node. Multiple names can point at the same underlying node, and unnamed nodes can still exist as part of the graph.

This matters when reading a DAG:

- `nodes` is the full graph inventory
- `names` is the convenient lookup table
- `result` is the node treated as the DAG's final answer

## Function calls produce nested DAG structure

When you call a runnable, DaggerML creates a function DAG for that execution and a `FnNode` in the caller. The `FnNode` points at the called DAG, and the called DAG records its own `argv` and terminal result or error.

That gives DaggerML a durable record of how one computation led to another. Cross-DAG links stay explicit instead of becoming hidden object pointers.

## How to think about it

It helps to think of a DAG as a replayable, inspectable computation artifact:

- values become literal nodes
- function calls become function-result nodes
- imported results stay marked as imports
- the final answer is a named or unnamed node selected as `result`

When you inspect a committed DAG through the public API, not every nested dict/list subvalue necessarily has its own persisted node identity. The API can therefore expose read-only `Projection` values for committed collection traversal. A projection lets you keep drilling into nested structure and ask for:

- the selected subvalue with `.value()`
- the provenance DAG behind that subvalue with `.context(root=...)`

That keeps committed DAG interrogation read-only while still letting callers move from a projected value back to the nearest or rooted non-builtin function/import context that produced it.

See also:

- [Codecs and values](codecs-and-values.md)
- [Execution](execution.md)
- [Commits and history](commits-and-history.md)
