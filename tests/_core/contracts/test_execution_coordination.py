from __future__ import annotations

import json
import threading
import time
from datetime import timedelta
from types import SimpleNamespace

import pytest

from daggerml._core.db import Ref
from daggerml._core.exec_state import ExecutionState
from daggerml._core.types import CanceledExecutionError, DmlRepoError, Runnable, Uri
from tests._core.helpers import FakeCasStore, FakeExecutionRemote, run_parallel


def _state(cache_key: str | None = "cache") -> ExecutionState:
    state = object.__new__(ExecutionState)
    state.root_uri = "s3://bucket/root"
    state.n_workers = 1
    state.cache_key = cache_key
    state._store = FakeCasStore()
    state._remote = FakeExecutionRemote()
    return state


def _record(execution_id: str, lifecycle: str = "running", **updates) -> dict:
    metadata = {
        "execution_id": execution_id,
        "cache_key": updates.pop("cache_key", "cache"),
        "argv_ref": updates.pop("argv_ref", "node-argv:argv"),
        "created_at": 0,
    }
    state = {
        "lifecycle": lifecycle,
        "result_ref": updates.pop("result_ref", None),
        "result_source": updates.pop("result_source", None),
        "spawned_execution_ids": updates.pop("spawned_execution_ids", []),
        "child_execution_ids": updates.pop("child_execution_ids", []),
        "cancelation": updates.pop("cancelation", None),
        "invalidation": updates.pop("invalidation", None),
        "updated_at": 0,
    }
    if state["result_ref"] is not None and state["result_source"] is None:
        state["result_source"] = "runtime"
    driver = {
        "lock": updates.pop("lock", None),
        "not_before": updates.pop("not_before", None),
        "adapter_state": updates.pop("adapter_state", None),
        "cleanup": updates.pop("cleanup", None),
    }
    assert not updates
    return {"metadata": metadata, "state": state, "driver": driver}


def test_execution_record_requires_exact_split_sections() -> None:
    state = _state()
    with pytest.raises(DmlRepoError, match="record"):
        state.create_execution_record({"execution_id": "exec"})
    record = _record("extra")
    record["legacy"] = {}
    with pytest.raises(DmlRepoError, match="record"):
        state.create_execution_record(record)
    record = _record("exec")
    record["state"]["legacy"] = True
    with pytest.raises(DmlRepoError, match="state"):
        state.create_execution_record(record)


def test_split_sections_are_independently_inspectable() -> None:
    state = _state()
    assert state.create_execution_record(_record("exec"))

    assert state._snapshot(state._execution_key("exec", "metadata")).json == _record("exec")["metadata"]
    assert state._snapshot(state._execution_key("exec", "state")).json == _record("exec")["state"]
    assert state._snapshot(state._execution_key("exec", "driver")).json == _record("exec")["driver"]


def test_execution_edges_use_only_plural_exact_path() -> None:
    state = _state()

    assert state._record_edge("caller", "callee")

    key = "root/exec/edges/callee/caller.json"
    assert state._store._get(key) == '{"callee_execution_id":"callee","caller_execution_id":"caller"}'
    assert not any("/edge/" in candidate for candidate in state._store.objects)
    assert state.list_execution_callers("callee") == ["caller"]

    state._store._put_js("root/exec/edge/other/legacy.json", {})
    assert state.list_execution_callers("other") == []


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"caller_execution_id": "other", "callee_execution_id": "callee"},
        {"caller_execution_id": "caller", "callee_execution_id": "other"},
        {"caller_execution_id": "caller", "callee_execution_id": "callee", "extra": True},
    ],
)
def test_execution_edge_reads_reject_malformed_payloads(payload) -> None:
    state = _state()
    state._store._put_js("root/exec/edges/callee/caller.json", payload)

    with pytest.raises(DmlRepoError, match="Invalid execution edge"):
        state.list_execution_callers("callee")


@pytest.mark.parametrize("part", ["metadata", "state", "driver"])
def test_direct_inspection_rejects_each_partial_split_record(part) -> None:
    state = _state()
    assert state.create_execution_record(_record("exec"))
    assert state._store._delete(state._execution_key("exec", part))

    with pytest.raises(DmlRepoError, match=f"No execution {part}"):
        state.read_execution_record("exec")


def test_driver_lock_allows_one_parallel_owner_and_stale_owner_cannot_mutate() -> None:
    state = _state()
    assert state.create_execution_record(_record("exec"))
    metadata_etag = state._snapshot(state._execution_key("exec", "metadata")).etag
    state_etag = state._snapshot(state._execution_key("exec", "state")).etag
    owners = run_parallel(4, lambda _: state.acquire("exec"))
    owner = next(item for item in owners if item is not None)
    assert sum(item is not None for item in owners) == 1
    assert state._snapshot(state._execution_key("exec", "metadata")).etag == metadata_etag
    assert state._snapshot(state._execution_key("exec", "state")).etag == state_etag
    assert not state.unlock("exec", "stale")
    assert state.unlock("exec", owner)


def test_driver_lock_expiry_uses_s3_timestamps() -> None:
    state = _state()
    assert state.create_execution_record(_record("exec", lock={"owner": "old", "ttl": 1.0}))
    state._store.now += timedelta(seconds=2)
    assert state.acquire("exec") is not None


def test_state_updates_use_state_cas_without_driver_ownership() -> None:
    state = _state()
    assert state.create_execution_record(_record("exec", lock={"owner": "driver", "ttl": 300.0}))
    state._mutate_state("exec", lambda value: value.update(adapter_state={}) if False else None)
    assert state.read_execution_record("exec")["driver"]["lock"]["owner"] == "driver"


def test_reservation_loser_only_removes_its_owned_parts() -> None:
    state = _state()
    execution_id, owner, _ = state.reserve_execution(Ref("node-argv:argv"))
    state._store.now += timedelta(seconds=301)
    assert state.acquire(execution_id) is not None
    state._delete_reserved_execution(execution_id, owner)
    assert state.read_execution_record(execution_id)["metadata"]["execution_id"] == execution_id


def test_reservation_cleanup_preserves_all_parts_when_one_changed() -> None:
    state = _state()
    execution_id, owner, _ = state.reserve_execution(Ref("node-argv:argv"))
    state._mutate_state(execution_id, lambda value: value.update(updated_at=1))

    state._delete_reserved_execution(execution_id, owner)

    assert state.read_execution_record(execution_id)["state"]["updated_at"] > 0


def test_resolve_or_create_publishes_record_before_cache_pointer() -> None:
    state = _state()
    execution_id, owner, created = state._resolve_or_create(Ref("node-argv:argv"))
    assert created and owner is not None
    assert state.read_execution_record(execution_id)["metadata"]["argv_ref"] == "node-argv:argv"
    assert state._read_cache("cache")[0] == execution_id


def test_invalidation_conditionally_deletes_pointer_then_marks_state() -> None:
    state = _state()
    execution_id, owner, _ = state._resolve_or_create(Ref("node-argv:argv"))
    assert owner is not None
    state.unlock(execution_id, owner)
    response = state.invalidate_executions([execution_id], "user")
    assert response["invalidations"][0]["execution_id"] == execution_id
    assert state._read_cache("cache") is None
    assert state.read_execution_record(execution_id)["state"]["invalidation"]["requested_by"] == "user"


def test_invalidation_holds_driver_lock_and_does_not_rewrite_metadata(monkeypatch) -> None:
    state = _state()
    execution_id, owner, _ = state._resolve_or_create(Ref("node-argv:argv"))
    assert owner is not None
    state.unlock(execution_id, owner)
    metadata = state._snapshot(state._execution_key(execution_id, "metadata"))
    assert metadata is not None
    original_mutate = state._mutate_state

    def mutate(execution_id, fn, **kwargs):
        assert state.read_execution_record(execution_id)["driver"]["lock"] is not None
        return original_mutate(execution_id, fn, **kwargs)

    monkeypatch.setattr(state, "_mutate_state", mutate)

    state.invalidate_executions([execution_id], "user")

    current = state._snapshot(state._execution_key(execution_id, "metadata"))
    assert current is not None and current.etag == metadata.etag
    assert state.read_execution_record(execution_id)["driver"]["lock"] is None


def test_finish_publishes_result_independently_before_driver_finalizes() -> None:
    state = _state()
    assert state.create_execution_record(_record("exec", lock={"owner": "driver", "ttl": 300.0}))
    driver = state.read_execution_record("exec")["driver"]
    state.finish_execution("exec", Ref("dag:result"), None)
    record = state.read_execution_record("exec")
    assert record["state"]["result_ref"] == "dag:result"
    assert record["state"]["result_source"] == "runtime"
    assert record["state"]["lifecycle"] == "running"
    assert record["driver"] == driver
    state._finalize_runtime_result("exec", "driver")
    assert state.read_execution_record("exec")["state"]["lifecycle"] == "succeeded"


def test_cancel_wins_result_finalization_race() -> None:
    state = _state()
    assert state.create_execution_record(
        _record("exec", "cancel-pending", cancelation={"requested_by": "user", "requested_at": 1})
    )
    with pytest.raises(DmlRepoError):
        state.finish_execution("exec", Ref("dag:result"), None)


def test_runtime_publication_wins_cancelation_race() -> None:
    state = _state()
    assert state.create_execution_record(_record("exec", result_ref="dag:result"))
    assert state._create_cache("cache", "exec")

    assert state._plan_cancel(["exec"], "user") == ["exec"]

    record = state.read_execution_record("exec")
    assert record["state"]["lifecycle"] == "cancel-pending"
    assert record["state"]["result_ref"] == "dag:result"
    assert record["state"]["cancelation"]["requested_by"] == "user"
    assert state._read_cache("cache") is None


def test_invoke_failure_is_reused_as_cached_adapter_error(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("caller", cache_key=None, argv_ref=None))
    assert state.create_execution_record(_record("callee"))
    assert state._create_cache("cache", "callee")
    calls = []

    def call_adapter(request):
        calls.append(request["operation"])
        if request["operation"] == "invoke":
            return {"status": "provider-error", "error": "provider failed"}
        return {"status": "success"}

    monkeypatch.setattr(state, "_call_adapter", call_adapter)
    monkeypatch.setattr(state, "_error_dag", lambda *_: Ref("dag:error"))
    monkeypatch.setattr(state, "_runnable_for_execution", lambda *_: Runnable(Uri("target"), adapter="adapter"))

    first = state.get_or_start_fn(
        Ref("index:caller"), Runnable(Uri("target"), adapter="adapter"), Ref("node-argv:argv"), None
    )
    second = state.get_or_start_fn(
        Ref("index:caller"), Runnable(Uri("target"), adapter="adapter"), Ref("node-argv:argv"), None
    )

    execution_id = state._read_cache("cache")[0]
    record = state.read_execution_record(execution_id)
    assert first == second == Ref("dag:error")
    assert calls == ["invoke", "cleanup"]
    assert record["state"]["lifecycle"] == "failed"
    assert record["state"]["result_source"] == "adapter-error"
    assert record["driver"]["cleanup"] == {"status": "complete", "error": None}


def test_cleanup_records_complete_and_failure_statuses(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("complete", "succeeded", result_ref="dag:result"))
    assert state.create_execution_record(_record("failed", "succeeded", result_ref="dag:result"))
    monkeypatch.setattr(state, "_runnable_for_execution", lambda *_: Runnable(Uri("target"), adapter="adapter"))
    responses = iter(({"status": "success"}, {"status": "failure", "error": "cleanup failed"}))
    monkeypatch.setattr(state, "_call_adapter", lambda *_: next(responses))
    state._drive_cleanup("complete", None)
    state._drive_cleanup("failed", None)
    assert state.read_execution_record("complete")["driver"]["cleanup"] == {"status": "complete", "error": None}
    assert state.read_execution_record("failed")["driver"]["cleanup"] == {"status": "failed", "error": "cleanup failed"}


def test_shared_retry_delay_defers_cleanup_and_invoke(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("exec", "succeeded", result_ref="dag:result", not_before=10**15))
    monkeypatch.setattr(state, "_call_adapter", lambda *_: pytest.fail("adapter must not run"))
    state._drive_cleanup("exec", None)
    assert state.read_execution_record("exec")["driver"]["cleanup"] is None


@pytest.mark.parametrize(
    ("updates", "adapter_calls"),
    [
        ({"lock": {"owner": "other", "ttl": 300.0}}, 0),
        ({"cleanup": {"status": "failed", "error": "cleanup failed"}}, 0),
    ],
)
def test_cached_result_is_reusable_with_pending_or_failed_cleanup(monkeypatch, updates, adapter_calls) -> None:
    state = _state()
    assert state.create_execution_record(_record("exec", "succeeded", result_ref="dag:result", **updates))
    assert state._create_cache("cache", "exec")
    calls = []
    monkeypatch.setattr(state, "_call_adapter", lambda request: calls.append(request) or {"status": "success"})

    assert state.get_cached_result("cache", None) == Ref("dag:result")

    record = state.read_execution_record("exec")
    assert len(calls) == adapter_calls
    assert record["state"]["lifecycle"] == "succeeded"
    assert record["state"]["result_ref"] == "dag:result"
    assert state._read_cache("cache")[0] == "exec"


def test_describe_graph_reads_split_reachable_state() -> None:
    state = _state()
    assert state.create_execution_record(
        _record("root", spawned_execution_ids=["running"], child_execution_ids=["done"])
    )
    assert state.create_execution_record(
        _record("running", cancelation={"requested_by": "user", "requested_at": 1})
    )
    assert state.create_execution_record(_record("done", "succeeded", result_ref="dag:result"))
    assert state.create_execution_record(_record("unrelated"))

    graph = state.describe_graph(["root"])

    assert graph["roots"] == ["root"]
    assert set(graph["nodes"]) == {"root", "running", "done"}
    assert graph["nodes"]["root"]["spawned"] == ["running"]
    assert graph["nodes"]["root"]["children"] == ["done"]
    assert graph["nodes"]["running"]["cancel_requested_by"] == "user"
    assert graph["nodes"]["done"]["lifecycle"] == "succeeded"


def test_describe_graph_rejects_missing_execution() -> None:
    state = _state()

    with pytest.raises(DmlRepoError, match="No execution metadata"):
        state.describe_graph(["missing"])


@pytest.mark.parametrize("operation", ["describe", "invalidate"])
def test_partial_execution_records_fail_closed(operation) -> None:
    state = _state()
    assert state.create_execution_record(_record("exec"))
    state._store._delete(state._execution_key("exec", "driver"))

    with pytest.raises(DmlRepoError, match="driver"):
        if operation == "describe":
            state.describe_graph(["exec"])
        else:
            state.invalidate_executions(["exec"], "user")


def test_unified_only_cache_pointer_is_stale_and_not_parsed() -> None:
    state = _state()
    assert state._create_cache("cache", "legacy")
    state._store._put_js(
        "root/exec/execution/legacy.json",
        {"result_ref": "dag:must-not-be-used", "argv_ref": "node-argv:must-not-be-used"},
    )

    assert state.describe_cache("cache") is None
    assert state._read_cache("cache") is None
    assert "root/exec/execution/legacy.json" in state._store.objects


@pytest.mark.parametrize("reuse", [False, True])
def test_failed_child_registration_removes_only_attempted_artifacts(monkeypatch, reuse) -> None:
    state = _state()
    assert state.create_execution_record(_record("caller", cache_key=None, argv_ref=None))
    if reuse:
        assert state.create_execution_record(_record("callee"))
        assert state._create_cache("cache", "callee")
    monkeypatch.setattr(state, "_update_child", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("fail")))

    with pytest.raises(RuntimeError, match="fail"):
        state.get_or_start_fn(
            Ref("index:caller"), Runnable(Uri("target"), adapter="adapter"), Ref("node-argv:argv"), None
        )

    assert not any("/edges/" in key for key in state._store.objects)
    if reuse:
        assert state.read_execution_record("callee")["metadata"]["execution_id"] == "callee"
        assert state._read_cache("cache")[0] == "callee"
    else:
        assert state._read_cache("cache") is None
        assert not any("/execution/" in key and "caller" not in key for key in state._store.objects)


def test_invalidation_propagates_only_to_current_callers() -> None:
    state = _state()
    for execution_id, cache_key in (("child", "child-key"), ("parent", "parent-key")):
        assert state.create_execution_record(_record(execution_id, cache_key=cache_key))
        assert state._create_cache(cache_key, execution_id)
    state._record_edge("parent", "child")

    response = state.invalidate_executions(["child"], "user")

    assert {item["execution_id"] for item in response["invalidations"]} == {"child", "parent"}
    assert state.read_execution_record("child")["state"]["invalidation"] is not None
    assert state.read_execution_record("parent")["state"]["invalidation"] is not None
    assert state._read_cache("child-key") is None
    assert state._read_cache("parent-key") is None


def test_adapter_wire_rejects_retired_running_status(monkeypatch) -> None:
    state = _state()
    monkeypatch.setattr("daggerml._core.exec_state.shutil.which", lambda _: "/adapter")
    monkeypatch.setattr(
        "daggerml._core.exec_state.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout='{"status":"running"}', stderr=""),
    )
    with pytest.raises(DmlRepoError, match="running.*unsupported"):
        state._call_adapter({"operation": "invoke", "runnable": {"adapter": "adapter"}, "adapter_state": None})


@pytest.mark.parametrize(
    ("response", "success_status"),
    [
        ({"status": "success"}, "success"),
        ({"status": "success", "error": None, "adapter_state": None}, "success"),
        ({"status": "retry", "adapter_state": {}}, "success"),
        ({"status": "retry", "adapter_state": {"attempt": 1}, "retry_after_ms": 0}, "success"),
        ({"status": "provider-error", "error": "diagnostic"}, "success"),
        ({"status": "cancelled"}, "cancelled"),
        ({"status": "retry", "adapter_state": {}}, "cancelled"),
        ({"status": "provider-error", "error": "diagnostic"}, "cancelled"),
    ],
)
def test_adapter_response_matrix_accepts_exact_operation_shapes(response, success_status) -> None:
    assert _state()._validate_adapter_response(response, success_status=success_status) == response


@pytest.mark.parametrize(
    ("response", "success_status"),
    [
        ({}, "success"),
        ({"status": "running", "adapter_state": {}}, "success"),
        ({"status": "success", "error": "contradiction"}, "success"),
        ({"status": "success", "retry_after_ms": 1}, "success"),
        ({"status": "success", "extra": True}, "success"),
        ({"status": "retry"}, "success"),
        ({"status": "retry", "adapter_state": {}, "error": "contradiction"}, "success"),
        ({"status": "retry", "adapter_state": {}, "retry_after_ms": None}, "success"),
        ({"status": "retry", "adapter_state": {}, "retry_after_ms": True}, "success"),
        ({"status": "provider-error"}, "success"),
        ({"status": "provider-error", "error": "diagnostic", "retry_after_ms": 1}, "success"),
        ({"status": "cancelled"}, "success"),
        ({"status": "success"}, "cancelled"),
    ],
)
def test_adapter_response_matrix_rejects_malformed_operation_shapes(response, success_status) -> None:
    with pytest.raises(DmlRepoError):
        _state()._validate_adapter_response(response, success_status=success_status)


@pytest.mark.parametrize(
    ("operation", "response"),
    [
        ("invoke", {"status": "success"}),
        ("cleanup", {"status": "success", "adapter_state": None}),
        ("cancel", {"status": "cancelled"}),
        ("invoke", {"status": "provider-error", "error": "diagnostic"}),
        ("cleanup", {"status": "retry", "adapter_state": {}, "retry_after_ms": 1}),
        ("cancel", {"status": "retry", "adapter_state": {}}),
    ],
)
def test_adapter_runtime_boundary_accepts_operation_specific_responses(monkeypatch, operation, response) -> None:
    state = _state()
    monkeypatch.setattr("daggerml._core.exec_state.shutil.which", lambda _: "/adapter")
    monkeypatch.setattr(
        "daggerml._core.exec_state.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(response), stderr=""),
    )

    assert state._call_adapter(
        {"operation": operation, "runnable": {"adapter": "adapter"}, "adapter_state": None}
    ) == response


def test_cancel_marks_selected_execution_and_preserves_terminal_pointer() -> None:
    state = _state()
    assert state.create_execution_record(_record("active", cache_key=None, argv_ref=None))
    assert state.create_execution_record(_record("done", "succeeded", result_ref="dag:result"))
    assert state._create_cache("cache", "done")
    state.cancel("active", "user", None)
    assert state.read_execution_record("active")["state"]["lifecycle"] == "canceled"
    assert state._read_cache("cache")[0] == "done"


def test_cancel_driver_runs_parallel_rounds_and_retries_only_failures(monkeypatch) -> None:
    state = _state()
    counts = {"a": 0, "b": 0}
    first_round = threading.Barrier(2)

    def invoke(execution_id, *_):
        counts[execution_id] += 1
        if counts[execution_id] == 1:
            first_round.wait(timeout=1)
        return execution_id == "a" or counts[execution_id] > 1

    monkeypatch.setattr(state, "_invoke_cancel_adapter", invoke)

    state._run_cancel_driver(["a", "b"], "user", None, max_retries=1)

    assert counts == {"a": 1, "b": 2}


def test_cancel_retry_persists_deadline_and_holds_lock(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("exec"))
    monkeypatch.setattr(state, "_runnable_for_execution", lambda *_: Runnable(Uri("target"), adapter="adapter"))
    calls = []

    def call_adapter(_request):
        record = state.read_execution_record("exec")
        assert record["driver"]["lock"] is not None
        calls.append(int(time.time() * 1000))
        if len(calls) == 1:
            return {"status": "retry", "adapter_state": {"attempt": 1}, "retry_after_ms": 40}
        assert record["driver"]["adapter_state"] == {"attempt": 1}
        assert calls[-1] >= record["driver"]["not_before"]
        return {"status": "cancelled"}

    monkeypatch.setattr(state, "_call_adapter", call_adapter)

    state.cancel("exec", "user", None, max_retries=1)

    record = state.read_execution_record("exec")
    assert len(calls) == 2
    assert record["state"]["lifecycle"] == "canceled"
    assert record["driver"]["lock"] is None
    assert record["driver"]["not_before"] is None


def test_cancel_exhaustion_preserves_pending_state_and_releases_lock(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("exec"))
    monkeypatch.setattr(state, "_runnable_for_execution", lambda *_: Runnable(Uri("target"), adapter="adapter"))
    monkeypatch.setattr(state, "_call_adapter", lambda *_: (_ for _ in ()).throw(RuntimeError("boom")))

    with pytest.raises(DmlRepoError, match="exec"):
        state.cancel("exec", "user", None, max_retries=1)

    record = state.read_execution_record("exec")
    assert record["state"]["lifecycle"] == "cancel-pending"
    assert record["driver"]["lock"] is None


def test_concurrent_cancel_calls_serialize_adapter_invocation(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("exec"))
    monkeypatch.setattr(state, "_runnable_for_execution", lambda *_: Runnable(Uri("target"), adapter="adapter"))
    calls = []

    def call_adapter(_request):
        calls.append("cancel")
        time.sleep(0.05)
        return {"status": "cancelled"}

    monkeypatch.setattr(state, "_call_adapter", call_adapter)

    assert run_parallel(2, lambda _: state.cancel("exec", "user", None)) == [None, None]
    assert calls == ["cancel"]


def test_cancel_adapter_accepts_only_cancelled_as_success() -> None:
    state = _state()

    assert state._validate_adapter_response({"status": "cancelled"}, success_status="cancelled")
    with pytest.raises(DmlRepoError, match="invalid for this operation"):
        state._validate_adapter_response({"status": "success"}, success_status="cancelled")


def test_state_mutation_retries_cas_conflict(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("exec"))
    owner = state.acquire("exec")
    assert owner is not None
    state._store.conflict_keys.add(state._execution_key("exec", "state"))
    state._mutate_state("exec", lambda value: value.update(lifecycle="cancel-pending"), owner=owner)
    assert state.read_execution_record("exec")["state"]["lifecycle"] == "cancel-pending"


@pytest.mark.parametrize(
    ("source", "target", "allowed"),
    [
        ("pending", "running", True),
        ("pending", "cancel-pending", True),
        ("running", "succeeded", True),
        ("running", "failed", True),
        ("running", "cancel-pending", True),
        ("cancel-pending", "canceled", True),
        ("pending", "failed", False),
        ("succeeded", "running", False),
        ("failed", "cancel-pending", False),
        ("canceled", "running", False),
    ],
)
def test_execution_state_authority_enforces_lifecycle_transition_matrix(source, target, allowed) -> None:
    state = _state()
    assert state.create_execution_record(_record("exec", source))
    owner = state.acquire("exec")
    assert owner is not None

    if allowed:
        state._mutate_state("exec", lambda value: value.update(lifecycle=target), owner=owner)
        assert state.read_execution_record("exec")["state"]["lifecycle"] == target
    else:
        with pytest.raises(DmlRepoError):
            state._mutate_state("exec", lambda value: value.update(lifecycle=target), owner=owner)


@pytest.mark.parametrize(
    ("lifecycle", "mutation", "raises"),
    [
        ("running", lambda value: value.update(result_ref="dag:result", result_source="runtime"), False),
        ("running", lambda value: value.update(spawned_execution_ids=["child"]), False),
        (
            "cancel-pending",
            lambda value: value.update(spawned_execution_ids=[], child_execution_ids=["child"]),
            False,
        ),
        ("pending", lambda value: value.update(result_ref="dag:result", result_source="runtime"), True),
        ("succeeded", lambda value: value.update(spawned_execution_ids=["child"]), True),
    ],
)
def test_execution_state_authority_limits_lock_free_result_and_lineage(lifecycle, mutation, raises) -> None:
    state = _state()
    spawned = ["child"] if lifecycle == "cancel-pending" else []
    assert state.create_execution_record(_record("exec", lifecycle, spawned_execution_ids=spawned))

    if raises:
        with pytest.raises(CanceledExecutionError):
            state._mutate_state("exec", mutation)
    else:
        state._mutate_state("exec", mutation)


def test_execution_state_authority_rejects_lost_driver_owner() -> None:
    state = _state()
    assert state.create_execution_record(_record("exec"))
    owner = state.acquire("exec")
    assert owner is not None
    driver, item = state._part_snapshot("exec", "driver")
    driver["lock"] = {"owner": "replacement", "ttl": 300.0}
    state._store._put_js(item, driver)

    with pytest.raises(DmlRepoError, match="ownership lost"):
        state._mutate_state("exec", lambda value: value.update(lifecycle="failed"), owner=owner)


@pytest.mark.parametrize("lifecycle", ["pending", "running", "cancel-pending", "succeeded", "failed", "canceled"])
def test_cancel_planning_uses_exact_lifecycle_selection_matrix(lifecycle) -> None:
    state = _state()
    assert state.create_execution_record(_record("exec", lifecycle))
    before = state._snapshot(state._execution_key("exec", "state")).etag

    selected = state._plan_cancel(["exec"], "user")
    record = state.read_execution_record("exec")
    if lifecycle in {"pending", "running"}:
        assert selected == ["exec"]
        assert record["state"]["lifecycle"] == "cancel-pending"
    elif lifecycle == "cancel-pending":
        assert selected == ["exec"]
        assert state._snapshot(state._execution_key("exec", "state")).etag == before
    else:
        assert selected == []
        assert record["state"]["lifecycle"] == lifecycle


@pytest.mark.parametrize("lifecycle", ["pending", "running", "succeeded", "failed"])
def test_cancel_driver_warns_and_drops_unexpected_lifecycle(monkeypatch, caplog, lifecycle) -> None:
    state = _state()
    assert state.create_execution_record(_record("exec", lifecycle))
    monkeypatch.setattr(state, "_call_adapter", lambda *_: pytest.fail("adapter must not run"))

    with caplog.at_level("WARNING"):
        state._run_cancel_driver(["exec"], "user", None, max_retries=0)

    assert "exec" in caplog.text and lifecycle in caplog.text
