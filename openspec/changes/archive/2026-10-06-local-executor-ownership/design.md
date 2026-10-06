## Context

See [proposal.md](proposal.md) for motivation. Core already persists opaque `adapter_state` in the execution's S3 driver record and supplies it to invoke, cleanup, and cancel. No additional persistence mechanism is needed.

Script launches a detached supervisor and returns its PID and local file paths. Docker returns a local-daemon container ID and temporary image reference, then runs its nested adapter with `--poll`. SSH forwards adapter operations and state without owning detached work. Batch also uses the nested `--poll` driver, but its own resources are remotely addressable.

Currently `AdapterBase.cli()` feeds retry state back into invoke, but its final response includes nested state. It also replaces the invocation response with the cleanup response and does not capture terminal invoke state updates before cleanup. Docker terminal polling returns the child's response directly rather than preserving its own state.

## Goals / Non-Goals

**Goals:**

- Prevent a different execution environment from inspecting, signaling, or deleting local script/Docker resources.
- Use flat launch state and the existing detached polling boundary, without special script behavior inside Docker.
- Keep SSH transparent and separate published invocation outcomes from cleanup diagnostics.
- Keep the implementation small and require the new state contract without compatibility branches.

**Non-Goals:**

- Ownership trees, executor-state stacks, new S3 state records, or changes to core execution/cache coordination.
- Automatic takeover, remote routing to an owner, polling-driver recovery, or new cancellation propagation.
- Ownership restrictions on SSH or Batch, shared/remote Docker daemon identification, boot-epoch/PID-reuse protection, or a new machine-identity service.
- Changes to execution scratch-path allocation.

## Decisions

### 1. Flat owner state identifies the environment that launches local work

Use `f"{os.geteuid()}@{socket.gethostname()}"` through a small private contrib helper, evaluated in the adapter process where script or Docker actually starts. The effective UID distinguishes users on the same machine without trusting username environment variables. This identity survives fresh adapter processes under the same effective user and requires no configuration, dependency, new files, or remote writes. Distinct hosts/containers must have distinct hostnames; default Docker container hostnames naturally distinguish containers from their host.

Script state becomes:

```json
{"owner":"1000@host-A","pid":123,"workdir":"...","result_path":"...","stdout_path":"...","stderr_path":"..."}
```

Docker state becomes:

```json
{"owner":"1000@host-A","container_id":"...","cleanup_image":null}
```

The identifier is not the runtime driver's lock-owner token and is not part of the runnable or cache key. Keep all backend fields at their existing level and preserve other continuation fields.

Alternatives: one execution-wide owner incorrectly pins SSH callers; per-process UUIDs break fresh-process polling; persistent machine tokens and structured ownership envelopes add machinery not needed for this change.

### 2. Guard local operations before any resource access

For object state, script and Docker compare required `state["owner"]` with the current environment before PID probes, file reads/removal, Docker executable discovery, or daemon commands. A different owner returns `{"status":"retry","adapter_state":state,"error":null}` through the normal executor response normalization. Do not claim success/cancelled, rewrite the owner, launch replacement work, or add special retry timing.

The guard applies to invoke continuation, cleanup, and cancel, including operations after result publication. Matching-owner behavior remains unchanged. Null-state cleanup/cancel retains its existing no-resource semantics; saved object state requires the new owner field. There is no ownerless-state fallback or migration.

The helper stays inside contrib. Do not add an ownership rule to `ExecutorBase`, because SSH must remain able to forward a remote executor's state from any calling machine.

### 3. The nested polling driver owns private continuation state

Change only the `--poll` invoke flow in `AdapterBase.cli()`:

1. Maintain the latest returned state locally, including state returned by terminal invoke. An omitted state field retains the last state; an explicit null replaces it.
2. Feed that state into repeated invoke and, after successful publication, into cleanup.
3. Maintain cleanup continuation independently while following the existing cleanup retry loop.
4. Once invoke and any required nested cleanup finish, emit the terminal invocation outcome without `adapter_state`.

Do not strip state in ordinary non-`--poll` calls or when a request has not completed this private invoke lifecycle. This preserves SSH forwarding and normal runtime continuation. Script has no awareness of `--poll`, Docker, or whether its returned state will be persisted to S3 or retained in a caller's memory.

Alternatives: changing script to synchronous execution inside Docker would create two execution paths; persisting every nested layer centrally would require a new state-composition protocol.

### 4. Docker returns only its own continuation state

Docker validates the nested terminal response, forwards its invocation status/error, discards any child continuation state, and attaches the original Docker state on every valid terminal outcome. Error paths with valid launch state also preserve that state. This retains owner, container ID, and image cleanup information for subsequent wrapper operations.

Batch's existing job-state handling remains compatible with child terminal responses that omit `adapter_state`; it needs no ownership restriction or new nested state. Verify that behavior in tests because the CLI change also affects Batch's nested driver.

### 5. Cleanup does not replace invocation outcome

Keep separate invocation and cleanup responses in the nested driver. Cleanup retries update only its private cleanup continuation. Terminal cleanup failure or an exception emits a diagnostic to stderr containing execution identity and the failure, while final output retains the invocation's status/error. Do not add new JSON response fields or write nested cleanup outcomes into core-owned execution files.

Keep the existing successful-invoke requirement for a published result before cleanup. Missing publication is still an invocation protocol error, not a cleanup warning. This change does not add a new cleanup lifecycle for failed nested invocations.

### 6. Composition follows detached boundaries, not runnable depth

| Runnable chain | Durable state received by the runtime caller | Private state |
| --- | --- | --- |
| `script` on A | Script owner A and launch state | None |
| `docker -> script` on A | Docker owner A and launch state | Container's `--poll` driver retains script state owned by that container |
| `ssh -> docker -> script`, SSH destination C | Docker owner C and launch state, transparently forwarded by SSH | Container's `--poll` driver retains script state |
| `docker -> ssh -> script` on A, SSH destination C | Docker owner A and launch state | Container's `--poll` driver retains script owner C and forwards operations to C |

Additional detached wrappers apply the same rule locally. No layer accumulates another layer's owner fields.

## Risks / Trade-offs

- [Duplicate hostnames] -> Treat distinct host/container names as an operating requirement and document it; do not present hostname ownership as authentication or a security boundary.
- [Owner disappears or its hostname/effective user changes] -> Other users/machines retry without touching local resources. No automatic failover is promised; publication can still make an existing result reusable, while cleanup/cancel may need owner participation.
- [Private driver exits unexpectedly] -> Its child continuation is lost. This is the existing ephemeral-driver limitation, not a durable recovery protocol.
- [Outer teardown leaves SSH-launched work running] -> Keep existing cancellation behavior and explicitly avoid claiming that container removal guarantees remote descendant teardown.
- [Owner-mismatch retries affect shared retry deadlines] -> Use the existing default retry policy; do not add long delays that unnecessarily block the actual owner.
- [Nested cleanup failure cannot be retained as per-layer S3 diagnostics] -> Report it on stderr without overwriting invocation success. Outer cleanup retains its existing separately coordinated outcome.
- [Mixed versions or ownerless saved state] -> Unsupported by explicit user decision. No fallback, dual-write, or legacy ownership guessing.

## Validation

Use contract tests to simulate A/B/C identities without provisioning multiple hosts. Assert that owner mismatches invoke no local resource functions for poll, cleanup, or cancel; fresh adapter instances with the same owner continue normally. Exercise private invoke/cleanup state updates, hidden terminal state, unchanged ordinary CLI responses, Docker wrapper-state preservation, and cleanup failure/exception diagnostics.

Verify SSH transparently forwards C-owned state from callers A and B, and Batch accepts state-free nested terminal responses while retaining job state. Use focused process/Docker integration tests for identical script behavior in both environments, recording unavailable infrastructure as unverified.

## References

- `DOC_MAP.md`
- `CONTRIBUTING.md`
- `docs/start-here/index.qmd`
- `docs/extend/adapters.qmd`
- `docs/extend/executors.qmd`
- `docs/use/execution.qmd`
- `docs/use/runtimes.qmd`
- `src/daggerml/README.md`
- `src/daggerml/_core/README.md`
- `openspec/README.md`
- `openspec/spec-overview.md`
- `openspec/specs/adapter-operation-protocol/spec.md`
- `openspec/specs/executor-cancellation/spec.md`
- `openspec/specs/execution-state/spec.md`
- `openspec/specs/docker-image-artifacts/spec.md`
