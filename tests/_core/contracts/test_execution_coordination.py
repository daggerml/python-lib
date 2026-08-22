from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest

from daggerml._core.db import Ref
from daggerml._core.exec_state import ExecutionState
from daggerml._core.types import DmlRepoError, Runnable, Uri
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
    owners = run_parallel(4, lambda _: state.acquire("exec"))
    owner = next(item for item in owners if item is not None)
    assert sum(item is not None for item in owners) == 1
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
    state._finalize_runtime_result("exec")
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

    assert state._plan_cancel(["exec"], "user") == []

    record = state.read_execution_record("exec")
    assert record["state"]["lifecycle"] == "succeeded"
    assert record["state"]["result_ref"] == "dag:result"
    assert record["state"]["cancelation"] is None
    assert state._read_cache("cache")[0] == "exec"


def test_invoke_failure_is_reused_as_cached_adapter_error(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("caller", cache_key=None, argv_ref=None))
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


def test_adapter_wire_accepts_only_success_retry_and_failure(monkeypatch) -> None:
    state = _state()
    monkeypatch.setattr("daggerml._core.exec_state.shutil.which", lambda _: "/adapter")
    monkeypatch.setattr(
        "daggerml._core.exec_state.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout='{"status":"running"}', stderr=""),
    )
    with pytest.raises(DmlRepoError, match="error text"):
        state._call_adapter({"operation": "invoke", "runnable": {"adapter": "adapter"}, "adapter_state": None})


def test_cancel_marks_selected_execution_and_preserves_terminal_pointer(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("active", cache_key=None, argv_ref=None))
    assert state.create_execution_record(_record("done", "succeeded", result_ref="dag:result"))
    assert state._create_cache("cache", "done")
    monkeypatch.setattr(state, "_invoke_cancel_adapter", lambda *_: "inactive")
    state.cancel("active", "user", None)
    assert state.read_execution_record("active")["state"]["lifecycle"] == "canceled"
    assert state._read_cache("cache")[0] == "done"


def test_state_mutation_retries_cas_conflict(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("exec"))
    state._store.conflict_keys.add(state._execution_key("exec", "state"))
    state._mutate_state("exec", lambda value: value.update(lifecycle="cancel-pending"))
    assert state.read_execution_record("exec")["state"]["lifecycle"] == "cancel-pending"
