# Flaky CI Investigation: Execution Cancellation and Docker over SSH

Date: 2026-08-23

This note records the investigation of two failures at commit
`7ade997ff6e6601391c2071c02a5985ba178b708`. The failures occurred in duplicate
GitHub Actions runs of the same commit, while the corresponding jobs succeeded
in the other run:

- [Python 3.12 test failure](https://github.com/daggerml/python-lib/actions/runs/32614978329/job/97134079552)
- [Examples failure](https://github.com/daggerml/python-lib/actions/runs/32614976448/job/97134074387)

The investigation used the GitHub CLI to inspect job metadata and logs, followed
by independent local reviews of execution-state coordination, nested executors,
and storage concurrency. No single root cause was proven for either failure,
but the review found several concrete correctness bugs and missing diagnostics.

## Observed Failures

### Cancellation integration test

`test_contrib_int_010__canceling_one_dag_preserves_shared_dependency_for_another`
failed after `Dml.runtime.cancel()` returned. The exclusive child execution was
still `running` rather than `canceled`:

```text
AssertionError: assert 'running' == 'canceled'
```

The assertion is not merely impatient. Public cancellation is synchronous: a
successful return must not leave an execution selected for cancellation in the
`running` lifecycle. Adding a post-cancellation sleep or polling loop would hide
an execution-state invariant violation.

### Docker-over-SSH example

`examples/python/02-ssh_docker_dataset.py` failed while invoking the nested
Docker executor over a local SSH server:

```text
daggerml.api.NodeError: docker container <id> exited without output
```

The Docker executor did not collect the container exit code, state error, or
logs. The exact inner failure therefore cannot be recovered from this Actions
run.

## Confirmed Correctness Bugs

### Child registration can race parent cancellation

In `ExecutionState.get_or_start_fn()`, the reverse caller edge is published
before the child is added to the parent's `spawned_execution_ids`. Parent
cancellation can snapshot and mark the parent between those operations:

1. Child registration creates the caller edge.
2. Parent cancellation snapshots state without the child in `spawned`.
3. Cancellation does not enqueue the child.
4. Child registration detects the canceled parent and removes its edge.
5. The child is left running without a caller and is not reconsidered.

Relevant code:

- `src/daggerml/_core/exec_state.py:725-734`
- `src/daggerml/_core/exec_state.py:829-830`
- `src/daggerml/_core/exec_state.py:884-892`
- `src/daggerml/_core/exec_state.py:1042-1098`

The failed integration test waits until the established graph is visible before
cancellation, so this specific interleaving probably does not explain that CI
failure. It remains a real short-duration product race requiring deterministic
coverage.

### Cancellation phase two fails open

`ExecutionState._invoke_cancel_adapter()` treats every lifecycle other than
`cancel-pending` as successful completion. If an invariant violation or stale
writer leaves a selected execution `pending` or `running`, the cancellation
driver removes it from the remaining set and returns successfully.

Relevant code:

- `src/daggerml/_core/exec_state.py:984-991`
- `src/daggerml/_core/exec_state.py:1100-1117`

No ordinary short-duration writer was found that can restore `running` in the
exact integration-test setup. Nevertheless, this fail-open branch is the direct
mechanism by which cancellation can report success for an invalid lifecycle.
No-work success should be restricted to explicitly accepted terminal states.

### Activation can overwrite cancellation after lease expiry

`mark_running()` unconditionally writes `running`. Driver locks have a fixed
300-second lease and no renewal, while code may hold them across unbounded
external operations. A stale activation owner can therefore overwrite
`cancel-pending` after another owner acquires an expired lease.

Relevant code:

- `src/daggerml/_core/index.py:95-138`
- `src/daggerml/_core/exec_state.py:390-434`
- `src/daggerml/_core/exec_state.py:699-701`

The lock duration excludes this as the trigger for the observed short test
failure, but the transition must still be guarded as `pending -> running`.

### Executors acknowledge cancellation before teardown

The script executor sends `SIGTERM`, removes its work directory immediately,
and returns `cancelled` without waiting for the process group to exit. Docker
cleanup and cancellation ignore `docker rm -f` failures. Runtime state can say
`canceled` while external work remains active.

Relevant code:

- `src/daggerml/contrib/executors/script.py:220-246`
- `src/daggerml/contrib/executors/docker.py:254-281`

Cancellation should wait for verified teardown, use a bounded escalation policy,
and return retry or failure when teardown cannot be confirmed.

### Docker inspect errors are misclassified as container exits

Any nonzero `docker inspect` return code is converted to the synthetic status
`exited`. If the scratch output is absent, polling returns terminal failure and
the core persists a reusable adapter-error DAG.

Relevant code:

- `src/daggerml/contrib/executors/docker.py:207-237`
- `src/daggerml/_core/exec_state.py:872-881`

A transient Docker daemon or CLI error can therefore produce the exact observed
message while the container is still running. Inspect transport errors, an
explicit missing container, and an explicitly exited container must be handled
separately.

### Nested adapter cleanup precedes output publication

The polling adapter completes a successful invocation, reads execution state,
drives nested cleanup, and only then writes `output.json`. An uncaught exception
during record reading, cleanup, or the final S3 write makes PID 1 exit without
an output object.

Relevant code:

- `src/daggerml/contrib/adapters.py:75-101`

This is a plausible cause class for the example failure. Ordinary worker
failures are converted into adapter responses, but failures in this final
control path escape before diagnostics are published.

### Nested responses replace Docker-owned state

Docker polling returns the nested executor response unchanged. Its
`adapter_state` belongs to the inner script executor, but the outer runtime
persists it in place of Docker's `{container_id, cleanup_image}` state. Later
Docker cleanup sees no container ID and silently skips resource removal. The
same state crosses the SSH layer unchanged.

Relevant code:

- `src/daggerml/contrib/executors/docker.py:221-225`
- `src/daggerml/contrib/executors/ssh.py:185-203`
- `src/daggerml/_core/exec_state.py:852-866`

Each executor layer must retain its own continuation and cleanup state rather
than exposing a nested executor's state as its own.

### Fresh success can bypass outer cleanup

When runtime result publication becomes visible during the coordinating call,
`get_or_start_fn()` finalizes and returns the result without driving outer
executor cleanup. Cleanup depends on a later lookup of the same cache key,
which may never occur.

Relevant code:

- `src/daggerml/_core/exec_state.py:630-648`
- `src/daggerml/_core/exec_state.py:831-834`
- `src/daggerml/_core/exec_state.py:865-871`

This can leak stopped containers and loaded images during successful examples.

## Additional Risks

- Driver locks have fixed leases without heartbeats and are held across adapter
  subprocesses. Lease expiry can duplicate external operations even when only
  one response is persisted.
- Different executions load and remove a shared Docker image tag. Cleanup can
  remove the tag between another execution's load and run operations.
- Every nonzero SSH exit is terminal. An ambiguous SSH 255 can occur after the
  remote side has launched work, producing a cached failure while that work
  continues.
- Script durable state identifies processes only by PID. PID reuse can make
  polling or cancellation inspect or signal an unrelated process.
- The SSH and Moto helpers reserve an ephemeral port by closing the reservation
  socket before the service binds, leaving a small bind race.

## Causal Assessment

### Cancellation failure

The graph-readiness barrier excludes the confirmed first-registration gap from
the observed test interleaving. With nonexpired locks and strongly consistent
edge operations, the established exclusive child should be selected and reach
`canceled` before `runtime.cancel()` returns.

The remaining explanations are an uninstrumented state invariant violation, an
unexpected incoming edge, or inconsistent edge listing/deletion behavior from
the test backend. The fail-open phase-two behavior allows such a violation to be
reported as success but does not identify the writer or omission that caused
it.

Cancellation instrumentation should record:

- the executions selected during planning;
- each child's incoming edges when it is considered;
- lifecycle values before and after phase two;
- lock owner and lease metadata for rejected transitions.

### Examples failure

The two most plausible classes are:

1. The container process encountered an uncaught exception before the final
   output write, including during cleanup or an S3 control operation.
2. `docker inspect` failed transiently and polling falsely classified the
   container as exited.

A normal successful S3 `PutObject` followed by a stale missing-object read is
not a convincing explanation: the write is synchronous, and the container does
not exit normally until it returns.

Future failures should capture:

- `docker inspect` status, exit code, error, and OOM state;
- `docker logs` before cleanup;
- nested adapter and supervisor stderr;
- whether `output.json` was absent or its write failed;
- Docker-owned adapter state before and after each nested response.

## Recommended Work Order

1. Make cancellation phase two reject `pending` and `running` after planning.
2. Guard activation as a `pending -> running` transition.
3. Add deterministic barriers around initial child registration and parent
   cancellation, then ensure orphaned children are reconsidered.
4. Make script and Docker cancellation verify teardown before returning
   `cancelled`.
5. Distinguish Docker inspect errors from explicit terminal status and collect
   container diagnostics before returning failure.
6. Preserve executor-owned state across nested executor responses.
7. Ensure terminal diagnostics are published even when nested cleanup fails.
8. Drive outer cleanup after fresh success without making result delivery depend
   on cleanup success.
9. Add lock renewal or avoid holding fixed leases over unbounded external calls.

## Verification Status

## Lifecycle Coordination Follow-up

The lifecycle coordination follow-up adopted a single guarded state-CAS
contract without changing execution record schemas. Lifecycle and control
writes now verify the current driver owner, while result publication and caller
lineage use narrowly allowlisted lock-free CAS operations. The transition table
is absorbing: `pending -> running`, `pending|running -> cancel-pending`,
`running -> succeeded|failed`, and `cancel-pending -> canceled`.

Child registration publishes its reverse edge and the caller's `running`-guarded
spawned summary before adapter invocation. Failed fresh registration cleans only
the matching cache pointer and unchanged records it owns. Normal terminal child
bookkeeping remains available to a `cancel-pending` caller so forward lineage
does not depend on race order. Cancellation Phase 2 accepts `canceled`, warns
with execution identity and lifecycle for other unexpected values, and drops
that work without adapter invocation or retry-set retention.

The exact lifecycle integration test passed in one local review run:

```text
1 passed in 11.85s
```

This confirms that the failure is intermittent; it does not invalidate the
correctness findings above. No deterministic reproducer for either original CI
failure has yet been added.
