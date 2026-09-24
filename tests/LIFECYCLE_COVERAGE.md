# Public lifecycle coverage

This inventory is maintained with the integration suite. Each row names the
public boundary, its focused-contract owner (where relevant), and the test that
must prove composition across persistence, remote, process, or installation
boundaries. An owner is passing only when its lifecycle assertions pass;
**deferred** expected failures and credential-gated acceptance without a run
are unverified. Exact invalid inputs, wire-shape matrices, and injected races
stay in the focused contracts; they do not need a second end-to-end matrix.

| Capability / public surface | Focused contracts | Deterministic lifecycle owner | External acceptance |
| --- | --- | --- | --- |
| `Dml.init`, reopen, config precedence, `status`, `show` | `_core/contracts/test_config_resolution.py`, `test_head_refs.py` | `_core/integration/test_repository_collaboration_integration.py` | — |
| `Dml` branches, upstreams, tags, log, diff, checkout, merge, rebase, revert | `_core/contracts/test_history_queries_contracts.py`, `test_merge_rebase_revert_contracts.py` | `_core/integration/test_repository_collaboration_integration.py` | — |
| Remote `clone`, `fetch`, `pull`, `push`, tracking, FF-only publishing | `_core/contracts/test_revision_resolution_contracts.py` | `_core/integration/test_remote_repo_sync_integration.py`, `test_repository_collaboration_integration.py` | — |
| Branch/tag/exact revisions; shallow clone/deepen/unshallow/merge parents | `_core/contracts/test_shallow_history_contracts.py` | `_core/integration/test_dml_clone_bootstrap_integration.py`, `test_remote_repo_sync_integration.py`, `test_shallow_merge_integration.py` (public tag publication and shallow-tip inspection **deferred: xfail**) | — |
| Dependency add/fetch/list, revision history/diff, DAG checkout, isolation | `_core/contracts/test_dependency_lifecycle_contracts.py` | `_core/integration/test_dependency_satellite_integration.py` | — |
| Local and remote garbage collection, shallow and runtime roots | `_core/contracts/test_local_gc_shallow_contracts.py`, `test_remote_gc_contracts.py` | `_core/integration/test_public_gc_integration.py` (shallow-tip inspection **deferred: xfail**), `contrib/integration/test_funk_lifecycle_integration.py` | — |
| `Dml.runtime` create/put/import/start/commit/freeze/unfreeze/list/describe/get | `_core/contracts/test_runtime_freezing_contracts.py` | `api/integration/test_api_live_runtime_integration.py` (local), `contrib/integration/test_funk_lifecycle_integration.py` (remote) | — |
| `Dml.runtime` graph/record/cancel, cache get/describe/invalidate, retries | `_core/contracts/test_execution_coordination.py`, `test_runtime_retry_contracts.py`, `test_cache_gc_surface_contracts.py` | `contrib/integration/test_funk_lifecycle_integration.py` | — |
| `Dml.dag` describe/get node/error/argv; committed history trees | `_core/contracts/test_dag_tree_contracts.py` | `api/integration/test_api_live_runtime_integration.py`, `_core/integration/test_repository_collaboration_integration.py` (history inspection) | — |
| Public `daggerml` new/load/resume/defaults/temporary/status, `Dag` put/require/commit/freeze/cancel and `Node` inspection/projections/errors | `api/contracts/test_api_*`, `test_api_defaults.py` | `api/integration/test_api_live_runtime_integration.py`, `test_api_remote_authoring_integration.py` | — |
| Local LMDB refs, typed serde, transaction durability, concurrent commits | `_core/contracts/test_types.py`, `test_serde_values.py`, `test_ref_lifecycle_contracts.py` | `_core/integration/test_db_concurrency_integration.py`, `test_parallel_branch_commits.py` | — |
| S3 CAS/remote object transfer, S3Store data/JSON/tar, artifact URIs | `_core/contracts/test_s3_cas_contracts.py`, `contrib/contracts/test_s3_store_contract.py` | `_core/integration/test_remote_roundtrip.py`, `contrib/integration/test_s3_store_integration.py` (Moto artifact/project round trip) | — |
| Literal, projection, delayed-action, dataframe and installed codec entry points | `api/contracts/test_api_codecs.py`, `contrib/contracts/test_delayed_action_codec_contract.py` | `api/integration/test_api_live_runtime_integration.py`, `contrib/integration/test_codec_roundtrip_integration.py` | — |
| Contrib adapter/executor discovery, status and JSON wire protocol | `contrib/contracts/test_adapter_registry_contract.py`, `test_executor_registry_contract.py`, `test_executor_base_contract.py` | `contrib/integration/test_adapter_runtime_integration.py`, `test_local_process_integration.py` | — |
| Script funk, dagclass, local adapter process, cache reuse/cancel | `contrib/contracts/test_script_executor_contract.py`, `test_funks_contract.py`, `test_supervisor_contract.py` | `contrib/integration/test_funk_lifecycle_integration.py`, `test_local_process_integration.py` | — |
| Docker wrapper, image tar, nested dagclass | `contrib/contracts/test_nested_executor_protocol_contract.py` | `contrib/integration/test_docker_lifecycle_integration.py` (5 passed locally); executable docs build | — |
| SSH wrapper and remote adapter | `contrib/contracts/test_ssh_executor_contract.py` | `contrib/integration/test_ssh_lifecycle_integration.py` (loopback OpenSSH and Moto) | — |
| Lambda adapter and Batch executor | `contrib/contracts/test_adapter_runtime_contract.py` | — | `external/test_aws_acceptance_integration.py` (unverified without credentials) |
| `dml` CLI: config/repository/history/DAG/runtime/cache/dependencies, entry points | `api/contracts/test_cli_*` | `distribution/test_installed_cli_integration.py` (installed init, author, inspect, configure, publish, clone); component lifecycle rows above own the remaining operation semantics | — |
| `dml-dashboard` launcher, registry, auth, static UI, API/inspection/cancel | `dashboard/test_*_contracts.py`, `api/contracts/test_dashboard_cli_contracts.py` | `distribution/test_installed_dashboard_integration.py` | — |
| Installed dashboard provider discovery, selection, render/cache/refresh | `dashboard/test_custom_dashboard_*_contracts.py` | `distribution/test_installed_dashboard_plugin_integration.py` | — |
| Wheel/sdist packaged assets, console scripts, executable course | `distribution/check_installed_docs.py`, `docs_build_test.py` | `distribution/test_installed_cli_integration.py`; `docs/build.sh` CI build | — |

Contract-only exceptions: exact CLI error/help text, invalid revision/ref shapes,
malformed adapter responses, failure injection, lock races, and serialization
edge cases are observable without multiple independent components; their named
contract modules above remain the authoritative owners. Plugin example behavior
is exercised by its installed-provider lifecycle rather than by a separate
example-specific distribution test.

Deferred product-behavior gaps (no passing owner yet):

- Public publication and clone of a tag pointing at a merge tip:
  `test_shallow_merge_integration.py::test_tagged_merge_tip_clones_by_public_tag`.
  `push` publishes the branch but has no public remote-tag publication path.
- Shallow-tip inspection when a parent is absent:
  `test_shallow_merge_integration.py::test_merge_tip_materializes_both_parents_and_deepens`
  and `test_public_gc_integration.py::test_local_gc_preserves_live_branch_tag_and_shallow_boundary`.
  `Dml.show` traverses the missing parent. The passing assertions in those
  modules cover other history and GC behavior, not shallow-tip inspection.

These strict expected failures remain test owners for follow-up product work;
neither an xfail nor a passing contract substitutes for a passing lifecycle.

The executable-docs build does not retain a stable final workspace: the build
render script deletes its disposable work tree on both success and failure.
Its inline assertions and `tests/docs_build_test.py` remain the acceptance
owner; duplicating the course or retaining its scratch workspace solely for
an extra assertion would not add a durable distribution guarantee.
