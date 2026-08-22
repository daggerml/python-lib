from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest

from daggerml._core import CanceledExecutionError
from daggerml._core.db import Ref
from daggerml._core.exec_state import ExecutionState
from daggerml._core.s3_cas import CasItemConflict
from daggerml._core.types import DmlRepoError, Runnable, Uri
from tests._core.helpers import FakeCasStore, FakeExecutionRemote, run_parallel


def _state(cache_key: str = "cache") -> ExecutionState:
    state = object.__new__(ExecutionState)
    state.root_uri = "s3://bucket/root"
    state.n_workers = 1
    state.cache_key = cache_key
    state._store = FakeCasStore()
    state._remote = FakeExecutionRemote()
    return state


def _record(execution_id: str, lifecycle="running", **updates):
    record = {
        "execution_id": execution_id,
        "cache_key": updates.pop("cache_key", "cache"),
        "lifecycle": lifecycle,
        "created_at": 0,
        "updated_at": 0,
        "lock": updates.pop("lock", None),
        "adapter_state": None,
        "argv_ref": updates.pop("argv_ref", "node-argv:argv"),
        "result_ref": None,
        "spawned_execution_ids": [],
        "child_execution_ids": [],
        "cancelation": None,
        "invalidation": None,
    }
    record.update(updates)
    return record


def test_execution_record_schema_rejects_missing_unified_fields() -> None:
    state = _state()
    with pytest.raises(DmlRepoError, match="Invalid execution record"):
        state.create_execution_record({"execution_id": "exec"})


def test_execution_record_schema_accepts_only_cancel_pending_intermediate() -> None:
    state = _state()
    assert state.create_execution_record(
        _record("pending", "cancel-pending", cancelation={"requested_by": "user", "requested_at": 1})
    )

    for execution_id, lifecycle in (("requested", "cancel-requested"), ("ready", "cancel-ready")):
        with pytest.raises(DmlRepoError, match="Invalid execution lifecycle"):
            state.create_execution_record(_record(execution_id, lifecycle))


def test_embedded_lock_allows_one_parallel_owner() -> None:
    state = _state()
    assert state.create_execution_record(_record("exec"))

    owners = run_parallel(4, lambda _: state.acquire("exec"))

    assert sum(owner is not None for owner in owners) == 1


def test_expiry_uses_s3_last_modified_and_date() -> None:
    state = _state()
    assert state.create_execution_record(_record("exec", lock={"owner": "old", "ttl": 10.0}))
    state._store.now += timedelta(seconds=11)

    owner = state.acquire("exec")

    assert owner is not None
    assert owner != "old"


def test_expired_unchanged_owner_can_still_mutate() -> None:
    state = _state()
    assert state.create_execution_record(_record("exec", lock={"owner": "mine", "ttl": 1.0}))
    state._store.now += timedelta(seconds=2)

    state._mutate("exec", "mine", lambda record: record.update(adapter_state={"poll": 1}))

    assert state.read_execution_record("exec")["adapter_state"] == {"poll": 1}


def test_stale_owner_cannot_mutate_or_unlock_after_steal() -> None:
    state = _state()
    assert state.create_execution_record(_record("exec", lock={"owner": "old", "ttl": 1.0}))
    state._store.now += timedelta(seconds=2)
    new_owner = state.acquire("exec")
    assert new_owner is not None

    with pytest.raises(CasItemConflict):
        state._mutate("exec", "old", lambda record: record.update(adapter_state={"stale": True}))
    assert not state.unlock("exec", "old")
    assert state.read_execution_record("exec")["lock"]["owner"] == new_owner


def test_unlock_clears_only_matching_owner() -> None:
    state = _state()
    assert state.create_execution_record(_record("exec"))
    owner = state.acquire("exec")
    assert owner is not None

    assert state.unlock("exec", owner)
    assert state.read_execution_record("exec")["lock"] is None


def test_execution_created_before_plain_cache_pointer() -> None:
    state = _state()

    execution_id, owner, created = state._resolve_or_create(Ref("node-argv:argv"))

    assert created
    assert owner is not None
    assert state.read_execution_record(execution_id)["argv_ref"] == "node-argv:argv"
    assert state._read_cache("cache")[0] == execution_id


def test_parallel_cache_claim_selects_one_execution() -> None:
    state = _state()

    claims = run_parallel(4, lambda _: state._resolve_or_create(Ref("node-argv:argv"))[0])

    assert len(set(claims)) == 1
    assert state._read_cache("cache")[0] == claims[0]


def test_dangling_cache_pointer_is_repaired() -> None:
    state = _state()
    assert state._create_cache("cache", "missing")

    execution_id, _, created = state._resolve_or_create(Ref("node-argv:argv"))

    assert created
    assert execution_id != "missing"
    assert state._read_cache("cache")[0] == execution_id


def test_current_execution_is_reused_after_unlock() -> None:
    state = _state()
    execution_id, owner, _ = state._resolve_or_create(Ref("node-argv:argv"))
    assert owner is not None
    state.unlock(execution_id, owner)

    resolved, next_owner, created = state._resolve_or_create(Ref("node-argv:other"))

    assert resolved == execution_id
    assert next_owner is None
    assert not created


def test_invalidation_removes_matching_pointer_before_marking_record() -> None:
    state = _state()
    execution_id, owner, _ = state._resolve_or_create(Ref("node-argv:argv"))
    assert owner is not None
    state.unlock(execution_id, owner)

    events = []
    delete_cache = state._delete_cache
    mutate = state._mutate
    state._delete_cache = lambda *args: (events.append("delete"), delete_cache(*args))[1]
    state._mutate = lambda *args: (events.append("mark"), mutate(*args))[1]

    response = state.invalidate_executions([execution_id], "user")

    assert response["invalidations"][0]["execution_id"] == execution_id
    assert events.index("delete") < events.index("mark")
    assert state.read_execution_record(execution_id)["invalidation"]["requested_by"] == "user"
    assert state._read_cache("cache") is None


def test_invalidation_preserves_rebound_pointer_while_marking_selected_execution() -> None:
    state = _state()
    execution_id, owner, _ = state._resolve_or_create(Ref("node-argv:argv"))
    assert owner is not None
    state.unlock(execution_id, owner)
    delete_cache = state._delete_cache

    def rebind_then_delete(cache_key, selected_execution_id):
        state._store._put(state._cache_key(cache_key), "replacement", overwrite=True)
        return delete_cache(cache_key, selected_execution_id)

    state._delete_cache = rebind_then_delete
    state.invalidate_executions([execution_id], "user")

    assert state._read_cache("cache")[0] == "replacement"
    assert state.read_execution_record(execution_id)["invalidation"] is not None


def test_cache_description_reports_exact_running_and_reusable_terminal_execution() -> None:
    state = _state()
    assert state.create_execution_record(_record("running"))
    assert state._create_cache("cache", "running")

    assert state.describe_cache("cache") == {
        "execution_id": "running",
        "result_ref": None,
        "lifecycle": "running",
    }

    state._store._delete(state._read_cache("cache")[1])
    assert state.create_execution_record(_record("done", "succeeded", result_ref="dag:result"))
    assert state._create_cache("cache", "done")
    assert state.describe_cache("cache") == {
        "execution_id": "done",
        "result_ref": "dag:result",
        "lifecycle": "succeeded",
    }


@pytest.mark.parametrize("marker", ["cancelation", "invalidation"])
def test_cache_description_hides_marked_terminal_result(marker) -> None:
    state = _state()
    record = _record("done", "succeeded", result_ref="dag:result")
    record[marker] = {"requested_by": "user", "requested_at": 1}
    assert state.create_execution_record(record)
    assert state._create_cache("cache", "done")

    assert state.describe_cache("cache") == {
        "execution_id": "done",
        "result_ref": None,
        "lifecycle": "succeeded",
    }


def test_cache_description_handles_absent_and_dangling_pointer() -> None:
    state = _state()
    assert state.describe_cache("cache") is None
    assert state._create_cache("cache", "missing")

    assert state.describe_cache("cache") is None
    assert state._read_cache("cache") is None


def test_cache_description_does_not_substitute_rebound_execution(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("selected"))
    assert state.create_execution_record(_record("replacement"))
    assert state._create_cache("cache", "selected")
    read_record = state.read_execution_record

    def rebind_then_read(execution_id):
        state._store._put(state._cache_key("cache"), "replacement", overwrite=True)
        return read_record(execution_id)

    monkeypatch.setattr(state, "read_execution_record", rebind_then_read)

    assert state.describe_cache("cache")["execution_id"] == "selected"
    assert state._read_cache("cache")[0] == "replacement"


def test_invalidation_propagates_to_current_caller_by_execution_id() -> None:
    state = _state()
    assert state.create_execution_record(_record("child", cache_key="child-key"))
    assert state.create_execution_record(_record("parent", cache_key="parent-key"))
    assert state._create_cache("child-key", "child")
    assert state._create_cache("parent-key", "parent")
    state._record_edge("parent", "child")

    response = state.invalidate_executions(["child"], "user")

    assert {item["execution_id"] for item in response["invalidations"]} == {"child", "parent"}
    assert state.read_execution_record("child")["invalidation"] is not None
    assert state.read_execution_record("parent")["invalidation"] is not None
    assert state._read_cache("child-key") is None
    assert state._read_cache("parent-key") is None


def test_invalidation_prunes_rebound_caller_and_its_ancestors() -> None:
    state = _state()
    for execution_id, cache_key in (
        ("child", "child-key"),
        ("historical", "parent-key"),
        ("replacement", "parent-key"),
        ("ancestor", "ancestor-key"),
    ):
        assert state.create_execution_record(_record(execution_id, cache_key=cache_key))
    assert state._create_cache("child-key", "child")
    assert state._create_cache("parent-key", "replacement")
    assert state._create_cache("ancestor-key", "ancestor")
    state._record_edge("historical", "child")
    state._record_edge("ancestor", "historical")

    response = state.invalidate_executions(["child"], "user")

    assert [item["execution_id"] for item in response["invalidations"]] == ["child"]
    assert state.read_execution_record("historical")["invalidation"] is None
    assert state.read_execution_record("replacement")["invalidation"] is None
    assert state.read_execution_record("ancestor")["invalidation"] is None
    assert state._read_cache("parent-key")[0] == "replacement"
    assert state._read_cache("ancestor-key")[0] == "ancestor"


def test_invalidation_marks_cacheless_root_and_deduplicates_missing_roots() -> None:
    state = _state()
    assert state.create_execution_record(_record("root", cache_key=None))

    response = state.invalidate_executions(["root", "missing", "root"], "user")

    assert response["invalidations"] == [
        {
            "execution_id": "root",
            "cache_key": None,
            "requested_by": "user",
            "requested_at": response["invalidations"][0]["requested_at"],
        }
    ]
    assert state.read_execution_record("root")["invalidation"] is not None


@pytest.mark.parametrize(
    "update",
    [
        lambda record: record.update(execution_id=""),
        lambda record: record.update(cache_key=""),
        lambda record: record.update(lifecycle=[]),
        lambda record: record.update(created_at=True),
        lambda record: record.update(updated_at=-1),
        lambda record: record.update(lock={"owner": "", "ttl": 1}),
        lambda record: record.update(lock={"owner": "owner", "ttl": float("inf")}),
        lambda record: record.update(argv_ref="dag:not-argv"),
        lambda record: record.update(result_ref="node-argv:not-dag"),
        lambda record: record.update(spawned_execution_ids=["child", "child"]),
        lambda record: record.update(spawned_execution_ids=["child"], child_execution_ids=["child"]),
        lambda record: record.update(cancelation={"requested_by": "", "requested_at": 0}),
        lambda record: record.update(invalidation={"requested_by": "user", "requested_at": True}),
    ],
)
def test_execution_record_validation_rejects_invalid_typed_fields(update) -> None:
    record = _record("exec")
    update(record)

    with pytest.raises(DmlRepoError):
        _state().create_execution_record(record)


def test_pre_adapter_failure_cleans_fresh_launch_artifacts() -> None:
    state = _state()
    state._add_spawned_execution = lambda *_: (_ for _ in ()).throw(RuntimeError("lineage failed"))

    with pytest.raises(RuntimeError, match="lineage failed"):
        state.get_or_start_fn(
            Ref("index:caller"), Runnable(Uri("target"), adapter="adapter"), Ref("node-argv:argv"), None
        )

    assert state._read_cache("cache") is None
    assert not any("/execution/" in key for key in state._store.objects)


def test_post_adapter_failure_retains_fresh_execution_record(monkeypatch) -> None:
    state = _state()
    state._add_spawned_execution = lambda *_: None

    def fail_after_call(_, *, on_call):
        on_call()
        raise RuntimeError("adapter failed")

    monkeypatch.setattr(state, "_call_adapter", fail_after_call)

    with pytest.raises(RuntimeError, match="adapter failed"):
        state.get_or_start_fn(
            Ref("index:caller"), Runnable(Uri("target"), adapter="adapter"), Ref("node-argv:argv"), None
        )

    execution_id = state._read_cache("cache")[0]
    assert state._snapshot(state._execution_key(execution_id)) is not None


def test_adapter_lookup_failure_cleans_fresh_execution_record(monkeypatch) -> None:
    state = _state()
    state._add_spawned_execution = lambda *_: None
    monkeypatch.setattr("daggerml._core.exec_state.shutil.which", lambda _: None)

    with pytest.raises(DmlRepoError, match="Adapter executable not found"):
        state.get_or_start_fn(
            Ref("index:caller"), Runnable(Uri("target"), adapter="missing"), Ref("node-argv:argv"), None
        )

    assert state._read_cache("cache") is None
    assert not any("/execution/" in key for key in state._store.objects)


def test_pre_adapter_failure_preserves_reused_execution_record() -> None:
    state = _state()
    execution_id, owner, _ = state._resolve_or_create(Ref("node-argv:argv"))
    assert owner is not None
    state.unlock(execution_id, owner)
    state._record_edge = lambda *_: (_ for _ in ()).throw(RuntimeError("edge failed"))

    with pytest.raises(RuntimeError, match="edge failed"):
        state.get_or_start_fn(
            Ref("index:caller"), Runnable(Uri("target"), adapter="adapter"), Ref("node-argv:argv"), None
        )

    assert state._read_cache("cache")[0] == execution_id
    assert state._snapshot(state._execution_key(execution_id)) is not None


def test_caller_registration_losing_to_cancel_pending_removes_edge_without_adapter_call(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("caller", cache_key=None))
    assert state.create_execution_record(
        _record("selected", "cancel-pending", cancelation={"requested_by": "user", "requested_at": 1})
    )
    assert state._create_cache("cache", "selected")
    monkeypatch.setattr(state, "_call_adapter", lambda *_args, **_kwargs: pytest.fail("adapter must not run"))

    with pytest.raises(CanceledExecutionError):
        state.get_or_start_fn(
            Ref("index:caller"), Runnable(Uri("target"), adapter="adapter"), Ref("node-argv:argv"), None
        )

    assert state.list_execution_callers("selected") == []
    assert state.read_execution_record("caller")["spawned_execution_ids"] == []
    assert state._read_cache("cache")[0] == "selected"


@pytest.mark.parametrize("stdout", ["not json", "[]", "{}", '{"status": 1}'])
def test_call_adapter_rejects_unrecoverable_malformed_output(monkeypatch, stdout) -> None:
    state = _state()
    monkeypatch.setattr("daggerml._core.exec_state.shutil.which", lambda _: "/adapter")
    monkeypatch.setattr(
        "daggerml._core.exec_state.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=stdout, stderr=""),
    )
    request = {"operation": "invoke", "runnable": {"adapter": "adapter"}, "adapter_state": None}

    with pytest.raises(DmlRepoError, match="[Aa]dapter response"):
        state._call_adapter(request)


def test_call_adapter_allows_omitted_terminal_or_cancel_state_and_existing_running_state(monkeypatch) -> None:
    state = _state()
    monkeypatch.setattr("daggerml._core.exec_state.shutil.which", lambda _: "/adapter")
    responses = iter(['{"status":"succeeded"}', '{"status":"cancelled"}', '{"status":"running"}'])
    monkeypatch.setattr(
        "daggerml._core.exec_state.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=next(responses), stderr=""),
    )

    assert state._call_adapter({"operation": "invoke", "runnable": {"adapter": "adapter"}, "adapter_state": None})[
        "status"
    ] == "succeeded"
    assert state._call_adapter({"operation": "cancel", "runnable": {"adapter": "adapter"}, "adapter_state": None})[
        "status"
    ] == "cancelled"
    assert state._call_adapter(
        {"operation": "invoke", "runnable": {"adapter": "adapter"}, "adapter_state": {"job": "1"}}
    )["status"] == "running"


def test_graph_reads_nested_cancelation_requester() -> None:
    state = _state()
    record = _record("root", cancelation={"requested_by": "user", "requested_at": 1})
    assert state.create_execution_record(record)

    graph = state.describe_graph(["root"])

    assert graph["nodes"]["root"]["cancel_requested_by"] == "user"


def test_cancel_driver_processes_cyclic_pending_execution_once(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(
        _record(
            "loop",
            "cancel-pending",
            cancelation={"requested_by": "user", "requested_at": 1},
            spawned_execution_ids=["loop"],
        )
    )
    dispatched = []
    monkeypatch.setattr(
        state, "_invoke_cancel_adapter", lambda execution_id, *_: dispatched.append(execution_id) or "cancelled"
    )

    selected = state._plan_cancel(["loop"], "user")
    state._run_cancel_driver(selected, "user", None)

    assert dispatched == ["loop"]
    assert state.read_execution_record("loop")["lifecycle"] == "canceled"


def test_cancel_finalizes_selected_descendants_leaves_first(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("d0", spawned_execution_ids=["f0", "f1"]))
    assert state.create_execution_record(_record("d1", spawned_execution_ids=["f1"]))
    assert state.create_execution_record(_record("f0"))
    assert state.create_execution_record(_record("f1"))
    state._record_edge("d0", "f0")
    state._record_edge("d0", "f1")
    state._record_edge("d1", "f1")
    dispatched = []
    monkeypatch.setattr(
        state, "_invoke_cancel_adapter", lambda execution_id, *_: dispatched.append(execution_id) or "cancelled"
    )

    response = state.cancel("d0", "user", None, mode="full")

    assert state.read_execution_record("d0")["lifecycle"] == "canceled"
    assert state.read_execution_record("f0")["lifecycle"] == "canceled"
    assert state.read_execution_record("f1")["lifecycle"] == "running"
    assert state.list_execution_callers("f1") == ["d1"]
    assert dispatched == ["f0", "d0"]
    assert response["timeout"] == []


def test_cancel_reconsiders_shared_descendant_after_last_selected_caller_releases_it(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(
        _record("root", cache_key=None, argv_ref=None, spawned_execution_ids=["a", "b"])
    )
    assert state.create_execution_record(_record("a", spawned_execution_ids=["shared"]))
    assert state.create_execution_record(_record("b", spawned_execution_ids=["shared"]))
    assert state.create_execution_record(_record("shared"))
    for caller, callee in (("root", "a"), ("root", "b"), ("a", "shared"), ("b", "shared")):
        state._record_edge(caller, callee)
    dispatched = []
    monkeypatch.setattr(
        state, "_invoke_cancel_adapter", lambda execution_id, *_: dispatched.append(execution_id) or "cancelled"
    )

    state.cancel("root", "user", None)

    assert dispatched == ["shared", "a", "b", "root"]
    assert all(
        state.read_execution_record(execution_id)["lifecycle"] == "canceled"
        for execution_id in ("root", "a", "b", "shared")
    )


def test_terminal_spawned_execution_does_not_block_active_sibling_cancellation(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(
        _record("root", cache_key=None, argv_ref=None, spawned_execution_ids=["active", "done"])
    )
    assert state.create_execution_record(_record("active"))
    assert state.create_execution_record(_record("done", "succeeded", result_ref="dag:result"))
    state._record_edge("root", "active")
    state._record_edge("root", "done")
    dispatched = []
    monkeypatch.setattr(
        state, "_invoke_cancel_adapter", lambda execution_id, *_: dispatched.append(execution_id) or "cancelled"
    )

    state.cancel("root", "user", None)

    assert dispatched == ["active", "root"]
    assert state.read_execution_record("active")["lifecycle"] == "canceled"
    assert state.read_execution_record("done")["lifecycle"] == "succeeded"


def test_cancel_drive_replays_cleanup_after_interrupted_phase_one(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(
        _record(
            "root",
            "cancel-pending",
            cache_key="root-key",
            argv_ref=None,
            cancelation={"requested_by": "user", "requested_at": 1},
            spawned_execution_ids=["child"],
        )
    )
    assert state.create_execution_record(_record("child", cache_key=None, argv_ref=None))
    assert state._create_cache("root-key", "root")
    state._record_edge("root", "child")
    monkeypatch.setattr(state, "_invoke_cancel_adapter", lambda *_: "inactive")

    state.cancel("root", None, None, mode="drive")

    assert state._read_cache("root-key") is None
    assert state.list_execution_callers("child") == []
    assert state.read_execution_record("root")["lifecycle"] == "canceled"
    assert state.read_execution_record("child")["lifecycle"] == "canceled"


def test_cancel_preserves_cache_pointer_rebound_during_phase_one(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("selected"))
    assert state._create_cache("cache", "selected")
    delete_cache = state._delete_cache

    def rebind_then_delete(cache_key, execution_id):
        state._store._put(state._cache_key(cache_key), "replacement", overwrite=True)
        return delete_cache(cache_key, execution_id)

    monkeypatch.setattr(state, "_delete_cache", rebind_then_delete)
    monkeypatch.setattr(state, "_invoke_cancel_adapter", lambda *_: "inactive")

    state.cancel("selected", "user", None)

    assert state._read_cache("cache")[0] == "replacement"
    assert state.read_execution_record("selected")["lifecycle"] == "canceled"


def test_cancel_finishes_phase_one_before_invoking_any_adapter(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(
        _record("root", cache_key=None, argv_ref=None, spawned_execution_ids=["child"])
    )
    assert state.create_execution_record(_record("child"))
    state._record_edge("root", "child")
    events = []
    delete_dependency = state.delete_execution_dependency

    def record_delete(**kwargs):
        events.append(("delete", kwargs["callee_execution_id"]))
        delete_dependency(**kwargs)

    def record_invoke(execution_id, *_):
        events.append(("invoke", execution_id))
        return "inactive"

    monkeypatch.setattr(state, "delete_execution_dependency", record_delete)
    monkeypatch.setattr(state, "_invoke_cancel_adapter", record_invoke)

    state.cancel("root", "user", None)

    assert events == [("delete", "child"), ("invoke", "child"), ("invoke", "root")]


def test_concurrent_cancel_drivers_converge_on_canceled(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("root", cache_key=None, argv_ref=None))
    dispatched = []
    monkeypatch.setattr(
        state, "_invoke_cancel_adapter", lambda execution_id, *_: dispatched.append(execution_id) or "inactive"
    )

    run_parallel(2, lambda _: state.cancel("root", "user", None))

    assert state.read_execution_record("root")["lifecycle"] == "canceled"
    assert 1 <= dispatched.count("root") <= 2


def test_cancel_phase_one_retries_cas_conflict(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("root", cache_key=None, argv_ref=None))
    wait_acquire = state._wait_acquire

    def acquire_then_conflict_once(execution_id):
        owner = wait_acquire(execution_id)
        state._store.conflict_keys.add(state._execution_key(execution_id))
        return owner

    monkeypatch.setattr(state, "_wait_acquire", acquire_then_conflict_once)
    monkeypatch.setattr(state, "_invoke_cancel_adapter", lambda *_: "inactive")

    state.cancel("root", "user", None)

    assert state.read_execution_record("root")["lifecycle"] == "canceled"


def test_cancel_phase_one_surfaces_cas_retry_exhaustion(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("root", cache_key=None, argv_ref=None))
    put_js = state._store._put_js

    def reject_cancel_pending(key, value, **kwargs):
        if isinstance(value, dict) and value.get("lifecycle") == "cancel-pending":
            raise CasItemConflict("cancel conflict")
        return put_js(key, value, **kwargs)

    monkeypatch.setattr(state._store, "_put_js", reject_cancel_pending)

    with pytest.raises(DmlRepoError, match="Failed to mutate execution after CAS retries"):
        state.cancel("root", "user", None)

    assert state.read_execution_record("root")["lifecycle"] == "running"


def test_cancel_phase_one_surfaces_lock_acquisition_failure(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("root", cache_key=None, argv_ref=None))
    monkeypatch.setattr(
        state, "_wait_acquire", lambda *_: (_ for _ in ()).throw(DmlRepoError("lock acquisition failed"))
    )

    with pytest.raises(DmlRepoError, match="lock acquisition failed"):
        state.cancel("root", "user", None)


def test_cancel_adapter_exception_leaves_selected_execution_pending(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("root", cache_key=None, argv_ref=None))
    monkeypatch.setattr(
        state, "_invoke_cancel_adapter", lambda *_: (_ for _ in ()).throw(RuntimeError("cancel failed"))
    )

    with pytest.raises(RuntimeError, match="cancel failed"):
        state.cancel("root", "user", None)

    assert state.read_execution_record("root")["lifecycle"] == "cancel-pending"


def test_cancel_drive_reconstructs_pending_root_and_children(monkeypatch) -> None:
    state = _state()
    root = _record(
        "root",
        "cancel-pending",
        cancelation={"requested_by": "user", "requested_at": 1},
        spawned_execution_ids=["child"],
    )
    assert state.create_execution_record(root)
    assert state.create_execution_record(
        _record("child", "cancel-pending", cancelation={"requested_by": "user", "requested_at": 1})
    )
    dispatched = []
    monkeypatch.setattr(
        state, "_invoke_cancel_adapter", lambda execution_id, *_: dispatched.append(execution_id) or "cancelled"
    )

    state.cancel("root", None, None, mode="drive")

    assert dispatched == ["child", "root"]
    assert state.read_execution_record("root")["lifecycle"] == "canceled"


@pytest.mark.parametrize("lifecycle", ["succeeded", "failed", "canceled"])
def test_cancel_accepts_terminal_cache_entries_without_deleting_pointer(lifecycle) -> None:
    state = _state()
    assert state.create_execution_record(_record("done", lifecycle, result_ref="dag:result"))
    assert state._create_cache("cache", "done")

    response = state.cancel("done", "user", None)

    assert all(not items for items in response.values())
    assert state._read_cache("cache")[0] == "done"


@pytest.mark.parametrize("lifecycle", ["succeeded", "failed"])
def test_cancel_skips_terminal_lifecycle_after_acquiring_lock(monkeypatch, lifecycle) -> None:
    state = _state()
    assert state.create_execution_record(_record("done"))
    assert state._create_cache("cache", "done")
    wait_acquire = state._wait_acquire

    def finish_before_cancel_mutation(execution_id):
        record = state.read_execution_record(execution_id)
        record["lifecycle"] = lifecycle
        state._store._put_js(state._execution_key(execution_id), record)
        return wait_acquire(execution_id)

    monkeypatch.setattr(state, "_wait_acquire", finish_before_cancel_mutation)

    response = state.cancel("done", "user", None)

    assert all(not items for items in response.values())
    assert state.read_execution_record("done")["lifecycle"] == lifecycle
    assert state._read_cache("cache")[0] == "done"
