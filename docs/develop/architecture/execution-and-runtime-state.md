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

`ExecutionState` stores three exact objects per attempt:
`execution/<execution-id>/metadata.json` for immutable identity,
`state.json` for lifecycle, results, lineage, and controls, and `driver.json`
for the owner lock, adapter continuation, shared retry delay, and cleanup
outcome. State and driver use separate conditional-update domains. The driver
lock serializes adapter calls and driver mutations only; semantic state writers
use guarded CAS without that lock. A funk runtime can therefore publish
`result_ref` with `result_source="runtime"` while a caller holds the driver
lock. The caller later finalizes that state to `succeeded`; adapter failures
atomically publish an adapter-error DAG and `failed`. Lock expiry uses S3
`LastModified + ttl <= Date`, not machine time.

`cache/<cache-key>` contains only the current execution ID from reservation
until cancelation or invalidation conditionally deletes it. All three execution
objects exist before pointer publication; a losing reservation conditionally
deletes only its unchanged objects. Legacy unified and partial split attempts
are stale. Caller/callee edges and adapter-owned `io/<execution-id>/` data
remain separate.

Invalidation is rooted in explicit execution IDs, not cache keys. An explicit
execution remains selected if its cache pointer is absent or has rebound. For a
caller reached through an execution edge, however, the current pointer must
still name that caller before it is selected: a missing or rebound pointer
prunes that branch, without selecting its replacement or traversing callers
above the pruned execution.
All runtime identities entering the public `Dml.runtime` namespace are `Ref`
values. That boundary validates the runtime namespace and passes only `ref.id()`
to `IndexOps` and `ExecutionState`, whose remote records and protocols remain
string-ID based.

An adapter receives `invoke`, `cleanup`, or `cancel` through an executable
boundary. Repeated `invoke` requests perform both launch and status inspection;
there is no wire-level `poll` operation, although executors retain an internal
`poll()` method. Invoke and cleanup return `success`, `retry`, or a nonempty
failure code. Retry requires durable object state and may provide
`retry_after_ms`; the caller stores a shared absolute `driver.not_before` that
all invoke, cleanup, and cancellation drivers respect.

The next operation is derived from current state: `cancel-pending` selects
cancel, an absent result selects invoke, and a result without a cleanup marker
selects cleanup. Cleanup success or failure is recorded in `driver.cleanup`
without changing lifecycle or result, so reusable cache results do not wait for
resource pruning. Normal teardown belongs in idempotent cleanup rather than
terminal invoke inspection. Cleanup remains demand-driven; there is no
background reconciler if every caller disappears.

Cancellation first locks and CAS-selects the
complete unreferenced descendant set as `cancel-pending`, conditionally deletes
matching cache pointers, and removes selected callers' outgoing edges. Caller
registration uses the same callee lock, so registration either publishes a
valid edge before selection or observes `cancel-pending` and stops. After
planning finishes, the runtime invokes selected adapters concurrently. Each
request waits for `driver.not_before` and holds its execution lock across the
external call and response persistence. Only `cancelled` CAS-transitions the
execution to `canceled`; retry and failure remain `cancel-pending` for bounded,
idempotent retries.
