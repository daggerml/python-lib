---
name: daggerml-extensions
description: Use when building or reviewing DaggerML adapters, executors, codecs, or plugins. Applies DML-specific boundary, lifecycle, and normalization conventions.
---

# DaggerML Extensions

Treat the DaggerML docs and current source as the authority for extension
contracts. Use this skill to decide **which layer owns behavior and how to
verify it**, not to replace the protocol or API reference.

## Put Behavior At The Right Boundary

- Use a codec to turn a Python value into stageable DML data. It does not
  execute work.
- Use an adapter for the runnable's transport boundary: the executable that
  exchanges one request and response with the runtime. An adapter can lower a
  delayed runnable itself; core does not require a contrib executor.
- Use a contrib executor when an adapter benefits from reusable backend
  lowering and execution lifecycle hooks. Core dispatches to the adapter
  executable, not directly to an `ExecutorBase` plugin.

Keep backend launch, observation, and teardown with the backend implementation.
The runtime owns execution records, locks, retry coordination, cache pointers,
result publication, lifecycle transitions, and cancellation. Do not make an
extension mutate those runtime-owned records directly.

## Make Runnable Lowering Explicit

At staging time, resolve the delayed runnable using its logical adapter and
requested executor URI. Validate backend options and whether a nested runnable
is supported. Return a concrete runnable whose adapter executable will be
available wherever execution occurs. Do not confuse the logical adapter key,
the requested URI, and the executable selected for runtime dispatch.

Leaf executors perform the innermost work; wrappers carry a nested runnable.
Forward identifiers, remote and scratch context, durable continuation state,
cleanup result context, and cancellation fields through wrappers. Dropping a
field can break retries or teardown even when the first invoke succeeds.

## Design For Repeated Operations

The adapter wire protocol has invoke, cleanup, and cancel operations; it has no
wire poll operation. A contrib executor may route an initial invoke to `start`
and later invokes with persisted adapter state to `poll`. Each operation may
arrive in a fresh process or be retried, so do not rely on instance variables
or process-local state to resume work. Use durable adapter state and make
repeated operations safe for the same execution ID.

Return a valid terminal, retry, or diagnostic failure response. A retry needs
serializable object state so another process can continue; a failure needs a
useful error. Validate protocol responses instead of relying on a backend's
happy path.

Treat cleanup and cancellation as separate responsibilities. Cleanup happens
after result publication and must not publish, invalidate, or alter that
result; do not put required teardown solely in a final poll. Cancellation
must tolerate retries and report completion only after backend teardown.

## Keep Codecs Narrow And Deterministic

Select only values the codec actually understands. Encoding must progress
toward a stageable literal, collection, reference, URI, or runnable; returning
the same input type cannot terminate normalization. Test nested shapes and
repeatability, not just one direct value.

When DML already has a suitable codec or artifact representation, prefer
preserving it over copying materialized data into another format.

## Register Where Resolution Happens

Adapter and executor entry points make contrib classes discoverable when
authoring and lowering delayed runnables. Codec entry points provide factories
for normalization. Install the adapter executable where the runtime will
dispatch it; importing a module alone does not register or deploy an
integration. Check discovered registrations and runnable lowering separately
from backend availability.

Script workers receive rendered function source and explicitly injected
dependencies, not the authoring module's imports or globals. Keep script
functions self-contained when an integration executes them.

## Verify Contracts Before Infrastructure

Test the boundary that changed: codec selection and normalization; resolved
runnable identity and nesting; operation payloads and response validation;
durable retry and fresh-process resume; repeated cleanup and cancel; and
wrapper forwarding. Verify plugin discovery from an installed distribution,
then exercise the actual backend where applicable. A plain Python function
test does not validate script-worker isolation or the runtime lifecycle.

Before finishing, ask whether each state transition belongs to the runtime or
the extension, and whether a repeated request can resume without hidden
process state.
