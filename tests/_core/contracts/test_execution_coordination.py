from __future__ import annotations

import json
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import daggerml._core.exec_state as exec_state_mod
from daggerml._core.db import Ref
from daggerml._core.exec_state import CancellationError, ExecutionState
from daggerml._core.s3_cas import CasItemConflict
from daggerml._core.types import BadExecutionStatusError, CanceledExecutionError, DmlDB, DmlRepoError, Runnable, Uri
from daggerml.contrib.adapters import LocalAdapter
from tests._core.helpers import FakeCasStore, FakeExecutionRemote, run_parallel


def _state(cache_key: str = "cache") -> ExecutionState:
    state = object.__new__(ExecutionState)
    state.root_uri = "s3://bucket/root"
    state.n_workers = 1
    state.cache_key = cache_key
    state._store = FakeCasStore()
    state._remote = FakeExecutionRemote()
    state._cas = {}
    return state


def _record(execution_id: str, spawned=None, lifecycle="running"):
    now = int(time.time())
    return {
        "execution_id": execution_id,
        "cache_key": execution_id + "-cache",
        "lifecycle": lifecycle,
        "updated_at": now,
        "created_at": now,
        "spawned_execution_ids": list(spawned or []),
        "child_execution_ids": [],
        "cancellation_requested_by": None,
    }


def test_same_cache_key_allows_at_most_one_lock_claimant() -> None:
    state = _state()

    claims = run_parallel(4, lambda _: state.lock(ttl=60))

    assert claims.count(True) == 1


def test_expired_lock_can_be_stolen() -> None:
    state = _state()
    assert state.lock(ttl=-1)

    assert state.lock(ttl=60)


def test_spawned_execution_add_drop_retries_cas_conflict() -> None:
    state = _state()
    state.create_execution_record(_record("caller"))
    key = state._key_for_execution("caller")
    state._store.conflict_keys.add(key)

    state._add_spawned_execution("caller", "callee")
    assert state.read_execution_record("caller")["spawned_execution_ids"] == ["callee"]

    state._store.conflict_keys.add(key)
    state._complete_spawned_execution("caller", "callee")
    assert state.read_execution_record("caller")["spawned_execution_ids"] == []
    assert state.read_execution_record("caller")["child_execution_ids"] == ["callee"]


def test_spawned_execution_registration_raises_after_cas_retry_exhaustion() -> None:
    state = _state()
    state.create_execution_record(_record("caller"))

    with patch.object(state, "update_execution_record", side_effect=CasItemConflict("caller")) as update, patch.object(
        exec_state_mod.time, "sleep"
    ) as sleep:
        with pytest.raises(DmlRepoError, match="Failed to register child execution callee"):
            state._add_spawned_execution("caller", "callee")

    assert update.call_count == exec_state_mod.COORDINATION_CAS_ATTEMPTS
    assert sleep.call_count == exec_state_mod.COORDINATION_CAS_ATTEMPTS
    assert state.read_execution_record("caller")["spawned_execution_ids"] == []


def test_spawned_execution_registration_stops_when_cancellation_wins_cas_conflict() -> None:
    state = _state()
    state.create_execution_record(_record("caller"))
    original = state.update_execution_record
    attempts = {"count": 0}

    def update_with_concurrent_cancellation(record):
        attempts["count"] += 1
        if attempts["count"] == 1:
            canceled = state.read_execution_record("caller")
            canceled["lifecycle"] = "cancel-requested"
            original(canceled)
            raise CasItemConflict("caller")
        return original(record)

    with patch.object(state, "update_execution_record", side_effect=update_with_concurrent_cancellation), patch.object(
        exec_state_mod.time, "sleep"
    ):
        with pytest.raises(CancellationError):
            state._add_spawned_execution("caller", "callee")

    assert state.read_execution_record("caller")["spawned_execution_ids"] == []


def test_completed_child_bookkeeping_raises_after_cas_retry_exhaustion_and_can_retry() -> None:
    state = _state()
    state.create_execution_record(_record("caller", spawned=["callee"]))

    with patch.object(state, "update_execution_record", side_effect=CasItemConflict("caller")), patch.object(
        exec_state_mod.time, "sleep"
    ):
        with pytest.raises(DmlRepoError, match="Failed to record completed child execution callee"):
            state._complete_spawned_execution("caller", "callee")

    assert state.read_execution_record("caller")["spawned_execution_ids"] == ["callee"]
    state._complete_spawned_execution("caller", "callee")
    assert state.read_execution_record("caller")["child_execution_ids"] == ["callee"]


def test_non_running_caller_cannot_spawn() -> None:
    state = _state()
    state.create_execution_record(_record("caller", lifecycle="canceled"))

    with pytest.raises(CancellationError):
        state._add_spawned_execution("caller", "callee")


@pytest.mark.parametrize(
    ("mode", "lifecycle"),
    [("activation", "pending"), ("mutation", "running")],
)
def test_require_mutation_accepts_allowed_lifecycle(mode: str, lifecycle: str) -> None:
    state = _state()
    state.create_execution_record(_record("exec", lifecycle=lifecycle))

    record = state.require_mutation("exec", db=None, mode=mode)

    assert record["lifecycle"] == lifecycle


@pytest.mark.parametrize(
    ("mode", "lifecycle"),
    [
        ("activation", "running"),
        ("activation", "succeeded"),
        ("activation", "failed"),
        ("mutation", "pending"),
        ("mutation", "succeeded"),
        ("mutation", "failed"),
    ],
)
def test_require_mutation_raises_bad_status_for_wrong_non_cancel_lifecycle(mode: str, lifecycle: str) -> None:
    state = _state()
    state.create_execution_record(_record("exec", lifecycle=lifecycle))

    with pytest.raises(BadExecutionStatusError):
        state.require_mutation("exec", db=None, mode=mode)


@pytest.mark.parametrize("lifecycle", ["cancel-ready", "canceled"])
def test_require_mutation_raises_canceled_without_drive_for_terminal_cancel_lifecycle(lifecycle: str) -> None:
    state = _state()
    state.create_execution_record(_record("exec", lifecycle=lifecycle))

    with patch.object(state, "cancel") as cancel:
        with pytest.raises(CanceledExecutionError):
            state.require_mutation("exec", db=None, mode="mutation")

    cancel.assert_not_called()


def test_require_mutation_drives_cancel_requested_before_raising() -> None:
    state = _state()
    state.create_execution_record(_record("exec", lifecycle="cancel-requested"))

    with patch.object(state, "cancel", return_value={}) as cancel:
        with pytest.raises(CanceledExecutionError):
            state.require_mutation("exec", db=None, mode="activation")

    cancel.assert_called_once_with("exec", None, None, mode="drive")


def test_update_requires_prior_read_and_conflicts_are_explicit() -> None:
    state = _state()
    record = _record("exec")
    state.create_execution_record(record)
    state.read_execution_record("exec")
    state._store.conflict_keys.add(state._key_for_execution("exec"))

    with pytest.raises(CasItemConflict):
        state.update_execution_record({**record, "spawned_execution_ids": ["child"]})


def test_fake_cas_covers_conditional_write_behavior() -> None:
    state = _state()

    assert state.create_execution_record(_record("exec"))
    assert not state.create_execution_record(_record("exec"))


def test_get_or_start_fn_reserves_pending_execution_before_active_publication() -> None:
    state = _state()
    state.create_execution_record(_record("caller"))

    with patch.object(
        state,
        "_call_adapter",
        return_value={"status": "running", "error": None, "state": {"token": "abc"}, "dag_id": None},
    ):
        resp = state.get_or_start_fn(
            Ref("index:caller"),
            Runnable(target=Uri("script"), kwargs={}, adapter="dml-local-adapter"),
            Ref("node-argv:argv"),
            db=None,
        )

    assert resp is None
    active = state._remote.get_active("cache", raw=True)
    assert active is not None
    active_id = active["meta"]["execution_id"]
    assert state.read_execution_record(active_id)["lifecycle"] == "pending"


def test_get_or_start_fn_replaces_stale_active_pointer_missing_record() -> None:
    state = _state()
    state.create_execution_record(_record("caller"))
    state._remote.active["cache"] = {"meta": {"execution_id": "stale"}, "argv": "node-argv:old"}

    with patch.object(
        state,
        "_call_adapter",
        return_value={"status": "running", "error": None, "state": {"token": "abc"}, "dag_id": None},
    ):
        resp = state.get_or_start_fn(
            Ref("index:caller"),
            Runnable(target=Uri("script"), kwargs={}, adapter="dml-local-adapter"),
            Ref("node-argv:new"),
            db=None,
        )

    assert resp is None
    active = state._remote.get_active("cache", raw=True)
    assert active is not None
    active_id = active["meta"]["execution_id"]
    assert active_id != "stale"
    assert state.read_execution_record(active_id)["lifecycle"] == "pending"


def test_get_or_start_fn_persists_adapter_error_with_growth_aware_write(tmp_path) -> None:
    path = tmp_path / "db"
    path.mkdir()
    db = DmlDB(str(path), 1024 * 1024, 64 * 1024 * 1024)
    db.init()
    state = _state()
    state.create_execution_record(_record("caller"))
    calls = 0
    original_write = db.write_with_growth

    def write_with_tracking(fn):
        nonlocal calls
        calls += 1
        return original_write(fn)

    with patch.object(db, "write_with_growth", side_effect=write_with_tracking), patch.object(
        state,
        "_call_adapter",
        return_value={"status": "failed", "error": "adapter failed", "state": None, "dag_id": None},
    ):
        dag_ref = state.get_or_start_fn(
            Ref("index:caller"),
            Runnable(target=Uri("script"), kwargs={}, adapter="dml-local-adapter"),
            Ref("node-argv:argv"),
            db,
        )

    assert calls == 1
    assert dag_ref is not None
    with db.tx(readonly=True) as txn:
        assert txn.get(txn.get(dag_ref).error).message == "adapter failed"


def test_get_or_start_fn_rolls_back_fresh_launch_when_registration_fails() -> None:
    state = _state()
    state.create_execution_record(_record("caller"))

    with patch.object(state, "_add_spawned_execution", side_effect=DmlRepoError("registration failed")), patch.object(
        state, "_call_adapter"
    ) as adapter:
        with pytest.raises(DmlRepoError, match="registration failed"):
            state.get_or_start_fn(
                Ref("index:caller"),
                Runnable(target=Uri("script"), kwargs={}, adapter="dml-local-adapter"),
                Ref("node-argv:argv"),
                db=None,
            )

    assert adapter.call_count == 0
    assert state._remote.active == {}
    assert list(state._store._iter(state._store._key_for("edge/"))) == []
    assert len(list(state._store._iter(state._store._key_for("state/")))) == 1


def test_get_or_start_fn_preserves_reused_launch_when_registration_fails() -> None:
    state = _state()
    state.create_execution_record(_record("caller"))
    state.create_execution_record(_record("shared", lifecycle="pending"))
    state._remote.put_active("cache", "shared", Ref("node-argv:argv"))

    with patch.object(state, "_add_spawned_execution", side_effect=DmlRepoError("registration failed")):
        with pytest.raises(DmlRepoError, match="registration failed"):
            state.get_or_start_fn(
                Ref("index:caller"),
                Runnable(target=Uri("script"), kwargs={}, adapter="dml-local-adapter"),
                Ref("node-argv:argv"),
                db=None,
            )

    assert state._remote.active["cache"]["meta"]["execution_id"] == "shared"
    assert state.read_execution_record("shared")["lifecycle"] == "pending"


def test_describe_graph_returns_reachable_active_and_terminal_descendants() -> None:
    state = _state()
    root = _record("root", spawned=["active"])
    root["child_execution_ids"] = ["done"]
    active = _record("active")
    done = _record("done", lifecycle="succeeded")
    done["child_execution_ids"] = ["leaf"]
    leaf = _record("leaf", lifecycle="failed")
    unrelated = _record("other")
    for record in [root, active, done, leaf, unrelated]:
        state.create_execution_record(record)

    graph = state.describe_graph(["root"])

    assert graph["roots"] == ["root"]
    assert set(graph["nodes"]) == {"root", "active", "done", "leaf"}
    assert graph["nodes"]["root"]["spawned"] == ["active"]
    assert graph["nodes"]["root"]["children"] == ["done"]
    assert graph["nodes"]["done"]["children"] == ["leaf"]


def test_describe_graph_missing_root_raises() -> None:
    state = _state()

    graph = state.describe_graph(["missing"])

    assert graph["roots"] == ["missing"]
    assert graph["nodes"]["missing"] == {
        "execution_id": "missing",
        "cache_key": None,
        "lifecycle": "pending",
        "updated_at": 0,
        "created_at": 0,
        "cancel_requested_by": None,
        "children": [],
        "spawned": [],
    }


def test_exec_state_calls_local_adapter_via_subprocess_envelope(monkeypatch) -> None:
    state = _state()
    calls = []

    class InlineExecutor:
        @staticmethod
        def handle(**kwargs):
            calls.append(kwargs)
            return {"status": "succeeded", "error": None, "state": None, "dag_id": "d" * 64}

    monkeypatch.setattr(exec_state_mod.shutil, "which", lambda adapter: adapter)
    monkeypatch.setattr("daggerml.contrib.executors._base.get_executor", lambda adapter, name: InlineExecutor)

    def fake_run(argv, *, input, text, capture_output):
        assert argv == ["dml-local-adapter"]
        assert text is True
        assert capture_output is True
        payload = json.loads(input)
        result = LocalAdapter.send(**payload)
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(result, separators=(",", ":"), sort_keys=True),
            stderr="",
        )

    monkeypatch.setattr(exec_state_mod.subprocess, "run", fake_run)

    request = {
        "operation": "invoke",
        "cache_key": "cache",
        "execution_id": "exec-1",
        "remote": {"root": "s3://bucket/root"},
        "runnable": {"target": {"uri": "script"}, "kwargs": {}, "adapter": "dml-local-adapter", "sub": None},
        "state": None,
        "scratch_uri": "s3://bucket/root/scratch/exec-1",
    }

    response = state._call_adapter(request)

    assert response == {"status": "succeeded", "error": None, "state": None, "dag_id": "d" * 64}
    assert calls == [request]


def test_exec_state_subprocess_shim_preserves_resume_state(monkeypatch) -> None:
    state = _state()
    seen_states = []

    class PollingExecutor:
        @staticmethod
        def handle(**kwargs):
            seen_states.append(kwargs["state"])
            if kwargs["state"] is None:
                return {"status": "running", "error": None, "state": {"token": "abc"}, "dag_id": None}
            return {"status": "succeeded", "error": None, "state": None, "dag_id": "d" * 64}

    monkeypatch.setattr(exec_state_mod.shutil, "which", lambda adapter: adapter)
    monkeypatch.setattr("daggerml.contrib.executors._base.get_executor", lambda adapter, name: PollingExecutor)

    def fake_run(argv, *, input, text, capture_output):
        assert argv == ["dml-local-adapter"]
        payload = json.loads(input)
        result = LocalAdapter.send(**payload)
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(result, separators=(",", ":"), sort_keys=True),
            stderr="",
        )

    monkeypatch.setattr(exec_state_mod.subprocess, "run", fake_run)

    base_envelope = {
        "operation": "invoke",
        "cache_key": "cache",
        "execution_id": "exec-1",
        "remote": {"root": "s3://bucket/root"},
        "runnable": {"target": {"uri": "script"}, "kwargs": {}, "adapter": "dml-local-adapter", "sub": None},
        "scratch_uri": "s3://bucket/root/scratch/exec-1",
    }

    running = state._call_adapter({**base_envelope, "state": None})
    terminal = state._call_adapter({**base_envelope, "state": running["state"]})

    assert running == {"status": "running", "error": None, "state": {"token": "abc"}, "dag_id": None}
    assert terminal == {"status": "succeeded", "error": None, "state": None, "dag_id": "d" * 64}
    assert seen_states == [None, {"token": "abc"}]


def test_plan_cancel_preserves_shared_child_callers() -> None:
    state = _state()
    root = _record("root", spawned=["left"])
    left = _record("left", spawned=["shared"])
    other = _record("other", spawned=["shared"])
    shared = _record("shared")
    for record in [root, left, other, shared]:
        state.create_execution_record(record)
    state._record_execution_dependency("root", "left")
    state._record_execution_dependency("left", "shared")
    state._record_execution_dependency("other", "shared")

    with patch.object(
        state,
        "_state_for_execution",
        side_effect=lambda execution_id: (state.read_execution_record(execution_id), state),
    ), patch.object(state, "lock", return_value=True), patch.object(state, "unlock", return_value=None):
        state._plan_cancel(["root"], "user")

    assert state.read_execution_record("root")["lifecycle"] == "cancel-requested"
    assert state.read_execution_record("left")["lifecycle"] == "cancel-requested"
    assert state.read_execution_record("shared")["lifecycle"] == "running"
    assert state.list_execution_callers("shared") == ["other"]


def test_plan_cancel_marks_pending_execution_cancel_pending() -> None:
    state = _state()
    state.create_execution_record(_record("root", spawned=["child"]))
    state.create_execution_record(_record("child", lifecycle="pending"))
    state._record_execution_dependency("root", "child")

    with patch.object(
        state,
        "_state_for_execution",
        side_effect=lambda execution_id: (state.read_execution_record(execution_id), state),
    ), patch.object(state, "lock", return_value=True), patch.object(state, "unlock", return_value=None):
        state._plan_cancel(["root"], "user")

    assert state.read_execution_record("root")["lifecycle"] == "cancel-requested"
    assert state.read_execution_record("child")["lifecycle"] == "cancel-requested"


def test_phase_one_moves_matching_active_manifest_to_cancel_target() -> None:
    state = _state()
    state.create_execution_record(_record("root"))
    state._remote.put_active("root-cache", "root", Ref("node-argv:argv"))

    with patch.object(
        state,
        "_state_for_execution",
        side_effect=lambda execution_id: (state.read_execution_record(execution_id), state),
    ), patch.object(state, "lock", return_value=True), patch.object(state, "unlock", return_value=None):
        state._plan_cancel(["root"], "user")

    assert state._remote.get_active("root-cache", raw=True) is None
    assert state._remote.get_cancel_target("root", raw=True) == {
        "meta": {"execution_id": "root"},
        "argv": "node-argv:argv",
    }


def test_phase_one_does_not_move_rebound_active_manifest() -> None:
    state = _state()
    state.create_execution_record(_record("root"))
    state._remote.put_active("root-cache", "replacement", Ref("node-argv:replacement"))

    with patch.object(
        state,
        "_state_for_execution",
        side_effect=lambda execution_id: (state.read_execution_record(execution_id), state),
    ), patch.object(state, "lock", return_value=True), patch.object(state, "unlock", return_value=None):
        state._plan_cancel(["root"], "user")

    assert state._remote.get_active("root-cache", raw=True)["meta"]["execution_id"] == "replacement"
    assert state._remote.get_cancel_target("root", raw=True) is None


def test_plan_cancel_blocks_until_coordination_lock_is_acquired() -> None:
    state = _state()
    state.create_execution_record(_record("root"))
    lock_attempts = {"count": 0}

    def lock_side_effect(ttl: float = 300.0) -> bool:
        lock_attempts["count"] += 1
        return lock_attempts["count"] >= 3

    with patch.object(
        state,
        "_state_for_execution",
        side_effect=lambda execution_id: (state.read_execution_record(execution_id), state),
    ), patch.object(state, "lock", side_effect=lock_side_effect), patch.object(
        state, "unlock", return_value=None
    ), patch.object(time, "sleep", return_value=None):
        state._plan_cancel(["root"], "user")

    assert lock_attempts["count"] == 3
    assert state.read_execution_record("root")["lifecycle"] == "cancel-requested"


def test_run_cancel_driver_sets_cancel_ready_and_calls_ready_children() -> None:
    state = _state()
    root = _record("root", spawned=["child"], lifecycle="cancel-requested")
    child = _record("child", lifecycle="cancel-ready")
    child["cache_key"] = "child-cache"
    state.create_execution_record(root)
    state.create_execution_record(child)

    with patch.object(
        state,
        "_state_for_execution",
        side_effect=lambda execution_id: (state.read_execution_record(execution_id), state),
    ), patch.object(
        state, "_invoke_cancel_adapter", return_value="cancelled"
    ) as invoke:
        plan = state._run_cancel_driver("root", "user", db=None, timeout_seconds=0.01)

    assert state.read_execution_record("root")["lifecycle"] == "cancel-ready"
    assert plan["cancelled"] == [{"cache_key": "child-cache", "execution_id": "child"}]
    invoke.assert_called_once_with("child", "user", None)


def test_run_cancel_driver_recursively_cancels_descendants_and_retains_spawned_lineage() -> None:
    state = _state()
    root = _record("root", spawned=["child"], lifecycle="cancel-requested")
    child = _record("child", spawned=["grandchild"], lifecycle="cancel-requested")
    grandchild = _record("grandchild", lifecycle="cancel-ready")
    for record in [root, child, grandchild]:
        state.create_execution_record(record)

    def fake_cancel(execution_id: str, requested_by: str | None, db) -> str:
        state._mark_execution_lifecycle(execution_id, "canceled")
        return "cancelled"

    with patch.object(
        state,
        "_state_for_execution",
        side_effect=lambda execution_id: (state.read_execution_record(execution_id), state),
    ), patch.object(state, "lock", return_value=True), patch.object(state, "unlock", return_value=None), patch.object(
        state, "_invoke_cancel_adapter", side_effect=fake_cancel
    ):
        state._run_cancel_driver("child", "user", db=None, timeout_seconds=1.0)
        plan = state._run_cancel_driver("root", "user", db=None, timeout_seconds=1.0)

    assert plan["cancelled"] == [{"cache_key": "child-cache", "execution_id": "child"}]
    assert state.read_execution_record("root")["lifecycle"] == "cancel-ready"
    assert state.read_execution_record("root")["spawned_execution_ids"] == ["child"]
    assert state.read_execution_record("root")["child_execution_ids"] == []
    assert state.read_execution_record("child")["lifecycle"] == "canceled"
    assert state.read_execution_record("child")["spawned_execution_ids"] == ["grandchild"]
    assert state.read_execution_record("child")["child_execution_ids"] == []
    assert state.read_execution_record("grandchild")["lifecycle"] == "canceled"


def test_complete_spawned_execution_allows_cancelled_parent_cleanup() -> None:
    state = _state()
    root = _record("root", spawned=["child"], lifecycle="cancel-requested")
    state.create_execution_record(root)

    state._complete_spawned_execution("root", "child")

    record = state.read_execution_record("root")
    assert record["lifecycle"] == "cancel-requested"
    assert record["spawned_execution_ids"] == []
    assert record["child_execution_ids"] == ["child"]


def test_finish_execution_rejects_cancel_lifecycle() -> None:
    state = _state()
    state.create_execution_record(_record("exec", lifecycle="cancel-requested"))

    with pytest.raises(CancellationError):
        state.finish_execution("exec", "dag:done", db=None)


def test_finish_execution_does_not_mark_success_when_transport_upload_fails() -> None:
    state = _state()
    state.create_execution_record(_record("exec"))

    def fail_upload(*args, **kwargs):
        raise OSError("S3 unavailable")

    state._remote.put_transport = fail_upload

    with pytest.raises(OSError, match="S3 unavailable"):
        state.finish_execution("exec", "dag:done", db=None)

    assert state.read_execution_record("exec")["lifecycle"] == "running"


def test_finish_execution_retries_conflict_and_observes_cancellation() -> None:
    state = _state()
    state.create_execution_record(_record("exec"))
    update = state.update_execution_record

    def concurrent_cancel(record):
        canceled = state.read_execution_record("exec")
        canceled["lifecycle"] = "cancel-requested"
        update(canceled)
        raise CasItemConflict("execution state")

    with patch.object(state, "update_execution_record", side_effect=concurrent_cancel):
        with pytest.raises(CancellationError):
            state.finish_execution("exec", "dag:done", db=None)


def test_cancel_drive_uses_stored_cancellation_requester() -> None:
    state = _state()
    record = _record("exec", lifecycle="cancel-ready")
    record["cancellation_requested_by"] = "user"
    state.create_execution_record(record)

    with patch.object(state, "_run_cancel_driver", return_value={}) as run_driver:
        state.cancel("exec", None, db=None, mode="drive")

    run_driver.assert_called_once_with("exec", "user", None)


@pytest.mark.parametrize("lifecycle", ["cancel-requested", "cancel-ready", "canceled"])
def test_set_canceled_accepts_cancel_lifecycles(lifecycle: str) -> None:
    state = _state()
    state.create_execution_record(_record("exec", lifecycle=lifecycle))

    state.set_canceled("exec")

    assert state.read_execution_record("exec")["lifecycle"] == "canceled"


def test_set_canceled_rejects_non_cancel_lifecycle() -> None:
    state = _state()
    state.create_execution_record(_record("exec", lifecycle="running"))

    with pytest.raises(DmlRepoError):
        state.set_canceled("exec")


def test_full_cancel_only_runs_phase_one() -> None:
    state = _state()
    state.create_execution_record(_record("root", lifecycle="running"))

    with patch.object(
        state,
        "_plan_cancel",
        side_effect=lambda execution_ids, requested_by: state._mark_execution_lifecycle(
            execution_ids[0], "cancel-requested", requested_by=requested_by
        ),
    ), patch.object(
        state,
        "_run_cancel_driver",
        return_value={"active-callers": [], "inactive": [], "cancelled": [], "timeout": [], "error": []},
    ):
        state.cancel("root", "user", db=None, mode="full")

    assert state.read_execution_record("root")["lifecycle"] == "cancel-requested"


def test_mark_execution_lifecycle_retries_cas_conflict() -> None:
    state = _state()
    state.create_execution_record(_record("exec", lifecycle="running"))
    state._store.conflict_keys.add(state._key_for_execution("exec"))

    state._mark_execution_lifecycle("exec", "cancel-requested", requested_by="user")

    record = state.read_execution_record("exec")
    assert record["lifecycle"] == "cancel-requested"
    assert record["cancellation_requested_by"] == "user"


def test_mark_execution_lifecycle_does_not_clobber_terminal_on_cas_retry() -> None:
    state = _state()
    state.create_execution_record(_record("exec", lifecycle="running"))
    original = state.update_execution_record
    attempts = {"count": 0}

    def update_with_concurrent_finish(record):
        attempts["count"] += 1
        if attempts["count"] == 1:
            terminal = state.read_execution_record("exec")
            terminal["lifecycle"] = "succeeded"
            original(terminal)
            raise CasItemConflict(state._key_for_execution("exec"))
        return original(record)

    with patch.object(state, "update_execution_record", side_effect=update_with_concurrent_finish):
        with pytest.raises(DmlRepoError, match="cannot transition from succeeded to cancel-requested"):
            state._mark_execution_lifecycle("exec", "cancel-requested", requested_by="user")

    assert state.read_execution_record("exec")["lifecycle"] == "succeeded"
    assert attempts["count"] == 1


def test_mark_execution_lifecycle_idempotent_when_already_target() -> None:
    state = _state()
    state.create_execution_record(_record("exec", lifecycle="cancel-requested"))

    record = state._mark_execution_lifecycle("exec", "cancel-requested", requested_by="user")

    assert record["lifecycle"] == "cancel-requested"
    assert record["cancellation_requested_by"] == "user"


def test_run_cancel_driver_skips_cancel_ready_transition_for_already_canceled_root() -> None:
    state = _state()
    root = _record("root", lifecycle="canceled")
    child = _record("child", lifecycle="canceled")
    root["spawned_execution_ids"] = ["child"]
    for record in [root, child]:
        state.create_execution_record(record)

    plan = state._run_cancel_driver("root", "user", db=None, timeout_seconds=0.01)

    assert plan == {"active-callers": [], "inactive": [], "cancelled": [], "timeout": [], "error": []}
    assert state.read_execution_record("root")["lifecycle"] == "canceled"


def test_cancel_ready_timeout_cancels_own_execution() -> None:
    state = _state()
    record = _record("root", lifecycle="cancel-ready")
    record["updated_at"] = 0
    state.create_execution_record(record)

    with patch.object(state, "_invoke_cancel_adapter", return_value="cancelled") as invoke:
        plan = state._run_cancel_driver("root", "user", db=None, timeout_seconds=0)

    invoke.assert_called_once_with("root", "user", None)
    assert plan["cancelled"] == [{"cache_key": "root-cache", "execution_id": "root"}]
    assert state.read_execution_record("root")["lifecycle"] == "canceled"
