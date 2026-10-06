## 1. Local script ownership

Contract: `specs/local-executor-ownership/spec.md`. Add only a private contrib helper `_current_owner() -> str`, returning `f"{os.geteuid()}@{socket.gethostname()}"`. Script launch state remains an object containing its existing fields plus required `owner: str`. Wire requests and method signatures remain unchanged, as defined in `openspec/specs/adapter-operation-protocol/spec.md`. Nonowner responses normalize to `{"status":"retry","adapter_state":<unchanged state>,"error":null}`.

- [x] 1.1 Add the private hostname helper and script launch ownership. Guard object-state poll, cleanup, and cancel before local resource access; keep null-state no-resource behavior and add no ownerless-state fallback. Update the script contract tests with valid owner-bearing state and cases covering launch identity, fresh matching-owner processes, and nonowner operations that perform no PID/file access.
- [x] 1.2 Validate the script checkpoint with `uv run --dev --all-extras pytest tests/contrib/contracts/test_script_executor_contract.py tests/contrib/contracts/test_nested_executor_protocol_contract.py -m 'not slow'`. Assert unchanged state and retry on mismatch, existing matching-owner success/failure/cancel behavior, and a required owner on launch. Update any synthetic script state in affected core contract tests without adding compatibility logic.

## 2. Docker ownership and wrapper-state preservation

Depends on section 1's helper. Contracts: `specs/local-executor-ownership/spec.md` and the Docker terminal-response requirement in `specs/adapter-operation-protocol/spec.md`. Docker state is `{"owner":str,"container_id":str,"cleanup_image":str|null,...}`; retain all existing continuation fields. A valid terminal nested outcome is returned with the original Docker adapter state, never the child's. No Docker/SSH/Batch runnable or request schema changes.

- [x] 2.1 Add Docker launch ownership and guard object-state poll, cleanup, and cancel before Docker discovery/commands or resource changes. Preserve the wrapper state on valid nested success/failure and local error responses with valid launch state. Add contract cases asserting that nonowner operations execute no local commands and that child adapter state cannot overwrite Docker owner/container/image fields.
- [x] 2.2 Validate with `uv run --dev --all-extras pytest tests/contrib/contracts/test_nested_executor_protocol_contract.py tests/_core/contracts/test_flaky_ci_reproducers.py -m 'not slow'`. Cover matching-owner lifecycle behavior, all three mismatch operations, state-free nested terminal output, nested output with child state, nested failure, and malformed nested output retaining existing protocol-error behavior. Update other affected synthetic Docker launch states to include owner.

## 3. Private polling continuation and independent cleanup outcome

Depends on section 2 so hidden child state cannot erase Docker continuation. Contracts: `specs/adapter-operation-protocol/spec.md` and `specs/executor-cancellation/spec.md`. Scope is `AdapterBase.cli()`'s private `--poll` invoke lifecycle; its `cli(argv: list[str] | None = None) -> int` signature and stdin/stdout/S3 transport remain unchanged. Terminal private output contains invocation status/error without `adapter_state`. Non-`--poll` responses and operations outside the private invocation lifecycle retain their existing shape.

- [x] 3.1 Retain latest invocation state through all responses, including terminal updates, omitted fields, and explicit null. Feed the resulting state to required nested cleanup, keep cleanup continuation independent, and omit state only from completed private invocation output. Add adapter CLI contract cases for each state transition and unchanged ordinary CLI forwarding.
- [x] 3.2 Preserve terminal invocation outcome separately from cleanup responses. Finish cleanup retries, report terminal cleanup failure/exception to stderr with execution identity, and retain invocation success/error in final output. Add cases for cleanup success, state-changing retries, failure codes, exceptions, failed invoke output, and missing result publication remaining a protocol error. Do not add cleanup after failed invocation or new JSON fields.
- [x] 3.3 Validate with `uv run --dev --all-extras pytest tests/contrib/contracts/test_adapter_runtime_contract.py tests/contrib/contracts/test_nested_executor_protocol_contract.py tests/contrib/contracts/test_aws_client_resilience_contract.py -m 'not slow'`. Assert hidden state on private success/failure, latest state in cleanup, stderr-only cleanup diagnostics, unchanged non-`--poll` state, preserved Docker wrapper state, and Batch job-state retention for nested output without adapter state.

## 4. Composition and process-boundary validation

Depends on sections 1–3. SSH forwarding and Batch ownership policy remain unchanged. Use A/B/C hostname mocks rather than adding a multi-host test harness. Integration scenarios retain existing slow/docker markers, bounded subprocess lifetimes, and fixture-owned teardown.

- [x] 4.1 Add or update SSH contract coverage showing that A and B forward C-owned script/Docker state unchanged and ownership is checked on C, not by the caller transport. Validate with `uv run --dev --all-extras pytest tests/contrib/contracts/test_ssh_executor_contract.py -m 'not slow'`; assert no SSH wrapper owner or new nesting state.
- [x] 4.2 Update owner-bearing state in `tests/contrib/integration/test_local_process_integration.py` and `tests/contrib/integration/test_docker_lifecycle_integration.py`. Assert script executes through its normal detached supervisor path in both environments, fresh adapter processes recognize the same owner, Docker durable state remains host-owned, and child terminal handoff omits state. Run `uv run --dev --all-extras pytest tests/contrib/integration/test_local_process_integration.py` and `uv run --dev --all-extras pytest tests/contrib/integration/test_docker_lifecycle_integration.py -m docker` where infrastructure is available; record skips/unrun checks as unverified.

Integration result: 3 local-process tests passed; 5 Docker cases skipped because Docker and `LIFECYCLE_DOCKER_IMAGE` were unavailable. Docker end-to-end behavior remains unverified locally.

## 5. Documentation and completion checks

Depends on the preceding behavior checkpoints. Keep documentation focused on ownership, the private polling boundary, and independent cleanup. No unrelated refactors, scratch allocation changes, dependencies, or legacy-state migration.

- [x] 5.1 Update `docs/extend/adapters.qmd` and `docs/extend/executors.qmd` for hidden private state, hostname ownership, unchanged SSH forwarding, wrapper-state preservation, and stderr cleanup diagnostics. Add narrowly necessary notes to `docs/use/execution.qmd` / `docs/use/runtimes.qmd` about owner participation in local polling/teardown, distinct host/container names, and unsupported ownerless state. Check examples against the unchanged public request/response schema.
- [x] 5.2 Run `openspec validate local-executor-ownership --strict`, then the required implementation finish sequence in order: `uv run --dev pyright`, `uv run --dev ruff check --fix .`, and `uv run --dev pytest -m 'not slow' .`. Review any lint edits and require all three code checks to pass; report infrastructure-dependent checks separately.

Completion result: strict OpenSpec validation, Pyright, and Ruff passed. The non-slow suite passed with 762 passed and 2 existing expected failures (136 deselected). The focused contrib/core contract run passed with 138 passed and 2 existing expected failures. Docker integration remains unverified as noted above.

Commit expectation: no commit or push during proposal authoring. When implementation commits are requested, keep each behavior checkpoint focused and include its matching tests; do not include unrelated work.

User-requested refinement: ownership now uses effective UID plus hostname. Contract tests cover a different effective user on the same host, the same effective user on a different host, and independence from username environment variables. No hostname-only compatibility path is added.

Refinement validation: Pyright and Ruff passed; the non-slow suite passed with 769 passed and 2 existing expected failures (136 deselected).
