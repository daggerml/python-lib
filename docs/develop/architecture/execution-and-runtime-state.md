# Execution and Runtime State

`IndexOps` in `_core/index.py` owns mutable runtime construction. A runtime
starts from a parent commit (or an empty tree), accumulates literals, imports,
function calls, names, results, and errors, then commits a finished DAG and,
when named, a new repository commit.

A user runtime can transition between active and frozen local representations.
Both retain the same runtime/execution ID and partial DAG ref; frozen state is
not an execution-record lifecycle state. This permits intermediate-DAG
inspection without making the DAG terminal or interrupting cancellation and
invalidation lineage.

For a runnable computation, `IndexOps` first builds argument nodes. Built-in
operations run against the local typed graph. Adapter-backed runnables create an
`ArgvNode`; its normalized DaggerML datum identity is the cache key. The core then asks
`ExecutionState` to reuse a completed cached DAG or coordinate a new execution.

`ExecutionState` stores one mutable `execution/<execution-id>.json` record per
attempt. It contains lifecycle, an embedded `{owner, ttl}` lock, adapter state,
argv and result refs, lineage summaries, cancelation, and invalidation. Every
mutation requires the current UUID owner and an S3 conditional update. Lock
expiry uses S3 `LastModified + ttl <= Date`, not machine time.

`cache/<cache-key>` contains only the current execution ID from reservation
until cancelation or invalidation conditionally deletes it. Caller/callee edges
and adapter-owned `io/<execution-id>/` data remain separate.
All runtime identities entering the public `Dml.runtime` namespace are `Ref`
values. That boundary validates the runtime namespace and passes only `ref.id()`
to `IndexOps` and `ExecutionState`, whose remote records and protocols remain
string-ID based.

An adapter receives an invocation or cancellation request through an executable
boundary. Invoke calls preserve object adapter state needed to continue running
work; cancellation responses may omit it. Repeated calls for one execution ID
are idempotent status checks. Cancellation is best-effort: deleting the current
cache pointer prevents
new callers from joining an execution that is being cancelled, but the adapter
may finish stopping it later.
