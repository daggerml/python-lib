# Execution and Runtime State

`IndexOps` in `_core/index.py` owns mutable runtime construction. A runtime
starts from a parent commit (or an empty tree), accumulates literals, imports,
function calls, names, results, and errors, then commits a finished DAG and,
when named, a new repository commit.

For a runnable computation, `IndexOps` first builds argument nodes. Built-in
operations run against the local typed graph. Adapter-backed runnables create an
`ArgvNode`; its normalized DaggerML datum identity is the cache key. The core then asks
`ExecutionState` to reuse a completed cached DAG or coordinate a new execution.

`ExecutionState` stores remote coordination records separately from local DAG
objects: advisory locks per cache key, immutable launch/resume state, mutable
execution lifecycle records, caller/callee lineage edges, invalidation markers,
and adapter scratch data. Lifecycle states include pending, running, succeeded,
failed, cancel-requested, cancel-ready, and canceled.

An adapter receives an invocation or cancellation request through an executable
boundary. It reports a lifecycle result and optional resume state or finished
DAG identity. Cancellation is best-effort: clearing active ownership prevents
new callers from joining an execution that is being cancelled, but the adapter
may finish stopping it later.
