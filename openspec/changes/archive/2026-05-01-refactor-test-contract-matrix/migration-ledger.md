# Test Migration Ledger: Contract Matrix

## Change

- OpenSpec change: `refactor-test-contract-matrix`
- Last updated: 2026-05-01

## Status Legend

- `planned` | `in_progress` | `blocked` | `done`

## Batch Plan

| Batch | Scope | Primary Files | Risk | Exit Criteria | Status |
|---|---|---|---|---|---|
| 1 | Low-risk contract suites | `tests/contrib/test_executor_base.py`, `tests/contrib/test_ssh_executor.py`, `tests/test_default_runtime.py` | low | Targeted migrated suites pass, `pytest -m "not slow"` pass, full `pytest` pass, mapping recorded | done |
| 2 | Lifecycle-heavy local runtime suites | `tests/contrib/test_local_runtime.py`, `tests/contrib/test_funkify.py` | medium | Stage-matrix parameterization complete, parity checks pass | done |
| 3 | Execution-state and internal integration-heavy suites | `tests/test_exec_state.py`, `tests/_internal/test_integration_roundtrip.py` | medium-high | Contract/integration split complete, parity checks pass | done |
| 4 | Infrastructure-heavy integration suites | `tests/contrib/test_ssh_integration.py` and remaining integration suites | high | Slow-marker compliance complete, parity checks pass | done |

## 1) Contract Coverage Mapping (Initial Batch 1)

| Contract ID | Contract Summary | Old Test Location(s) | New Test Location | Test Type | Slow? | Lifecycle Stages Covered | Status | Notes |
|---|---|---|---|---|---|---|---|---|
| EXB-HDL-001 | executor handle calls start when state is null | `tests/contrib/test_executor_base.py` | `tests/contracts/contrib/executor/test_executor_base_handle.py` | contract | no | kickoff | done | Implemented in lifecycle stage matrix test with canonical case ID. |
| EXB-HDL-002 | executor handle calls poll when state exists | `tests/contrib/test_executor_base.py` | `tests/contracts/contrib/executor/test_executor_base_handle.py` | contract | no | resume/poll | done | Implemented in lifecycle stage matrix test with canonical case ID. |
| EXB-HDL-003 | terminal start result is returned directly | `tests/contrib/test_executor_base.py` | `tests/contracts/contrib/executor/test_executor_base_handle.py` | contract | no | kickoff/terminal | done | Dedicated terminal passthrough assertion migrated. |
| EXB-HDL-004 | mixed state invocations route correctly | `tests/contrib/test_executor_base.py` | `tests/contracts/contrib/executor/test_executor_base_handle.py` | contract | no | kickoff + resume/poll | done | Mixed kickoff/resume assertions preserved. |
| SSH-RES-001 | local adapter resolves ssh runnable shape | `tests/contrib/test_ssh_executor.py` | `tests/contracts/contrib/executor/test_ssh_resolve_runnable.py` | contract | no | resolve | done | Canonical ID in test function name. |
| SSH-RES-002 | ssh resolve rejects invalid inputs | `tests/contrib/test_ssh_executor.py` | `tests/contracts/contrib/executor/test_ssh_resolve_runnable.py` | contract | no | resolve | done | Parametrized case IDs carry canonical contract ID. |
| SSH-HDL-001 | ssh handle forwards envelope to transport | `tests/contrib/test_ssh_executor.py` | `tests/contracts/contrib/executor/test_ssh_handle.py` | contract | no | kickoff | done | Included in stage matrix with canonical case ID. |
| SSH-HDL-002 | ssh transport nonzero exits map to failed | `tests/contrib/test_ssh_executor.py` | `tests/contracts/contrib/executor/test_ssh_handle.py` | contract | no | terminal-failed | done | Included in stage matrix with canonical case ID. |
| SSH-HDL-003 | running child result passes through | `tests/contrib/test_ssh_executor.py` | `tests/contracts/contrib/executor/test_ssh_handle.py` | contract | no | resume/poll | done | Included in stage matrix with canonical case ID. |
| SSH-HDL-004 | child failed result is projected unchanged | `tests/contrib/test_ssh_executor.py` | `tests/contracts/contrib/executor/test_ssh_handle.py` | contract | no | terminal-failed | done | Included in stage matrix with canonical case ID. |
| SSH-HDL-005 | runtime state is forwarded to child payload | `tests/contrib/test_ssh_executor.py` | `tests/contracts/contrib/executor/test_ssh_handle.py` | contract | no | resume/poll | done | Dedicated runtime-state forwarding test migrated. |
| DRT-STS-001 | status reports implicit default creation source | `tests/test_default_runtime.py` | `tests/contracts/runtime/test_default_runtime_status.py` | contract | no | runtime-init/status | done | Canonical ID in test function name. |
| DRT-STS-002 | process default is cached | `tests/test_default_runtime.py` | `tests/contracts/runtime/test_default_runtime_status.py` | contract | no | steady-state | done | Canonical ID in test function name. |
| DRT-STS-003 | scoped default temporarily overrides process default | `tests/test_default_runtime.py` | `tests/contracts/runtime/test_default_runtime_status.py` | contract | no | scoped lifecycle | done | Canonical ID in test function name. |
| DRT-STS-004 | top-level new/load delegates to default runtime | `tests/test_default_runtime.py` | `tests/contracts/runtime/test_default_runtime_status.py` | contract | no | operation dispatch | done | Canonical ID in test function name. |

## 2) Legacy Removal Plan (Initial Batch 1)

| Old File | Replacement New File(s) | Parity Evidence Required | Removal PR/Commit | Removed? | Notes |
|---|---|---|---|---|---|
| `tests/contrib/test_executor_base.py` | `tests/contracts/contrib/executor/test_executor_base_handle.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Legacy file removed after parity runs. |
| `tests/contrib/test_ssh_executor.py` | `tests/contracts/contrib/executor/test_ssh_resolve_runnable.py`, `tests/contracts/contrib/executor/test_ssh_handle.py` | targeted migrated suites pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Legacy file removed after parity runs. |
| `tests/test_default_runtime.py` | `tests/contracts/runtime/test_default_runtime_status.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Legacy file removed after parity runs. |
| `tests/contrib/test_local_runtime.py` | `tests/integration/contrib/test_local_runtime_integration.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Marked slow and moved to integration taxonomy. |
| `tests/contrib/test_funkify.py` | `tests/integration/contrib/test_funkify_integration.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Marked slow and moved to integration taxonomy. |
| `tests/test_exec_state.py` | `tests/integration/runtime/test_exec_state_integration.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Marked slow and moved to integration taxonomy. |
| `tests/_internal/test_integration_roundtrip.py` | `tests/integration/internal/test_roundtrip_integration.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Marked slow and moved to integration taxonomy with local fixture compatibility shim. |
| `tests/contrib/test_ssh_integration.py` | `tests/integration/contrib/test_ssh_integration.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Marked slow and moved to integration taxonomy. |
| `tests/contrib/test_s3_store.py` | `tests/integration/contrib/test_s3_store_integration.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Marked slow and moved to integration taxonomy. |
| `tests/contrib/test_supervisor.py` | `tests/integration/contrib/test_supervisor_integration.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Marked slow and moved to integration taxonomy. |
| `tests/contrib/test_funks.py` | `tests/integration/contrib/test_funks_integration.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Marked slow and moved to integration taxonomy. |
| `tests/_internal/ops/test_cache.py` | `tests/integration/internal/ops/test_cache_integration.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Marked slow and moved to integration taxonomy. |
| `tests/_internal/ops/test_remote.py` | `tests/integration/internal/ops/test_remote_integration.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Marked slow and moved to integration taxonomy. |
| `tests/_internal/ops/test_dml_project_workflows.py` (integration subset) | `tests/integration/internal/ops/test_dml_project_workflows_integration.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Extracted integration tests; contract-oriented tests remain in legacy file. |
| `tests/_internal/ops/test_commit.py` | `tests/integration/internal/ops/test_commit_integration.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Marked slow and moved to integration taxonomy. |
| `tests/_internal/ops/test_head.py` | `tests/integration/internal/ops/test_head_integration.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Marked slow and moved to integration taxonomy. |
| `tests/_internal/cli/test_init.py` (integration class) | `tests/_internal/cli/test_init.py` | targeted migrated suite pass + `pytest -m "not slow"` pass + full `pytest` pass + mapping complete | local working tree | yes | Integration class `TestInitCLIIntegration` marked slow in place. |

## 3) Parity Checklist (Per migrated family)

- [x] Canonical IDs are direct literals in test names/parameterized case IDs.
- [x] Lifecycle coverage is parameterized where the contract family spans stages.
- [x] Targeted migrated suites pass.
- [x] `pytest -m "not slow"` passes.
- [x] Full `pytest` passes.
- [x] Legacy tests removed only after parity evidence is recorded.

## 4) Decision Log

- 2026-04-30: Canonical contract IDs are direct literal strings (no shared ID indirection).
- 2026-04-30: Migration policy is full replacement; superseded legacy tests are removed after parity confirmation.
- 2026-04-30: Integration tests are marked `@pytest.mark.slow`.
- 2026-05-01: Batch 1-4 migration executed in this change; contract suites moved to `tests/contracts/`, integration-heavy suites moved to `tests/integration/` with `slow` marker.

## 5) Parity Evidence Log (Command-Level)

- 2026-05-01: Targeted migrated suites
  - Command: `uv run pytest tests/contracts/contrib/executor/test_executor_base_handle.py tests/contracts/contrib/executor/test_ssh_resolve_runnable.py tests/contracts/contrib/executor/test_ssh_handle.py tests/contracts/runtime/test_default_runtime_status.py`
  - Result: `17 passed`
- 2026-05-01: Targeted migrated integration suites
  - Command: `uv run pytest tests/integration/contrib/test_local_runtime_integration.py tests/integration/contrib/test_funkify_integration.py tests/integration/contrib/test_s3_store_integration.py tests/integration/contrib/test_supervisor_integration.py tests/integration/contrib/test_funks_integration.py tests/integration/internal/ops/test_cache_integration.py tests/integration/internal/ops/test_remote_integration.py tests/integration/internal/ops/test_dml_project_workflows_integration.py`
  - Result: `154 passed, 1 skipped`
- 2026-05-01: Targeted omitted migrated suites
  - Command: `uv run pytest tests/integration/contrib/test_ssh_integration.py tests/integration/runtime/test_exec_state_integration.py tests/integration/internal/test_roundtrip_integration.py`
  - Result: `34 passed`
- 2026-05-01: Targeted lifecycle matrix updates
  - Command: `uv run pytest tests/integration/contrib/test_funkify_integration.py tests/integration/internal/ops/test_dml_project_workflows_integration.py`
  - Result: `14 passed`
- 2026-05-01: Targeted remaining integration migration
  - Command: `uv run pytest tests/integration/internal/ops/test_commit_integration.py tests/integration/internal/ops/test_head_integration.py tests/_internal/cli/test_init.py::TestInitCLIIntegration`
  - Result: `19 passed`
- 2026-05-01: Fast-path parity (latest)
  - Command: `uv run pytest -m "not slow"`
  - Result: `498 passed, 209 deselected`
- 2026-05-01: Full parity (latest)
  - Command: `uv run pytest`
  - Result: `706 passed, 1 skipped`
- 2026-05-01: Fast-path parity
  - Command: `uv run pytest -m "not slow"`
  - Result: `517 passed, 189 deselected`
- 2026-05-01: Full parity
  - Command: `uv run pytest`
  - Result: `705 passed, 1 skipped`
