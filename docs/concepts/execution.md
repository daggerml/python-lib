# Execution

Execution in DaggerML means turning a runnable plus its arguments into another DAG.

## The input to execution

The public `Runnable` value holds:

- a `Uri` target
- an optional nested `sub` runnable
- keyword defaults
- an adapter name

Before execution starts, DaggerML normalizes the call inputs, stages them into the working DAG, and builds:

- an `ArgvNode` for positional inputs
- optionally a `KwargvNode` for keyword inputs

Those nodes are not extra metadata bolted on later. They are part of the persisted computation record and also drive cache identity.

## Builtins, cache, and adapters

`IndexOps.start_fn(...)` follows three broad paths:

1. Try a builtin implementation for supported `daggerml:` URIs.
2. If the call is not builtin, check for a cached DAG result keyed by the staged argv identity.
3. On a cache miss, coordinate adapter execution through remote-backed execution state.

That means function execution is not just "run a process." It is a repository operation that first asks whether this exact computation already has a known DAG result.

When execution crosses the adapter boundary, DaggerML sends an `AdapterInvokeRequest` containing the cache key, execution id, runnable spec, remote settings, and any saved resume state. `AdapterCancelRequest` is a separate operation: it carries the execution-owned argv pointer needed to cancel a detached attempt. Invocation responses are `running`, `succeeded`, or `failed`; cancellation responses are separate from runtime lifecycle state.

The important design point is that adapters do not get to redefine execution identity. The argv-backed cache key and the stored execution records remain the source of truth.

## Remote-backed execution state

For non-builtin execution, DaggerML uses remote state to coordinate work:

- a cache ref for completed results
- an active-execution pointer for in-flight work on the same cache key
- a cancel-target ref for an execution being canceled
- launch and lifecycle records for resume and status tracking

If another caller is already driving the same computation, a later caller can detect that and resume or wait rather than launching duplicate work.

Cancellation runs in two phases. Phase 1 locks each targeted cache key, marks the execution `cancel-requested`, and moves its active argv ref to an execution-owned cancel target. Phase 2 is distributed: runtimes wait for direct callees to become `cancel-ready`, cancel those callees, and then become ready themselves. A ready handoff times out after 60 seconds, allowing cleanup to be retried.

Execution records retain direct child lineage in two lists. `spawned_execution_ids` contains children that have not normally completed; `child_execution_ids` contains children that completed with either `succeeded` or `failed`. A canceled child remains in `spawned_execution_ids` so the canceled call graph remains inspectable.

## What success and failure mean

On success, DaggerML expects a DAG result to appear in cache and then links it back into the caller as a `FnNode`.

On failure, DaggerML materializes a failed DAG, publishes that terminal state, and raises the recorded DAG error back through the caller.

So even failures become part of the graph model instead of disappearing into logs.

## How to think about it

Execution is best understood as DAG production with caching and coordination around it:

- the call inputs become part of the DAG model
- the result is another DAG, not just an in-memory return value
- cache identity follows the staged call shape
- remote state lets multiple callers coordinate around the same work

See also:

- [DAGs and nodes](dags-and-nodes.md)
- [Remotes](remotes.md)
- [Codecs and values](codecs-and-values.md)
