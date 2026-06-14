from __future__ import annotations

import json
import time
from types import SimpleNamespace

import pytest

import daggerml._core.exec_state as exec_state_mod
from daggerml._core.exec_state import CancelledExecutionError, ExecutionState
from daggerml._core.s3_cas import CasItemConflict
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


def test_non_running_caller_cannot_spawn() -> None:
    state = _state()
    state.create_execution_record(_record("caller", lifecycle="cancelled"))

    with pytest.raises(CancelledExecutionError):
        state._add_spawned_execution("caller", "callee")


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
            return {"lifecycle": "succeeded", "error": None, "state": None, "dag_id": "d" * 64}

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

    envelope = {
        "cache_key": "cache",
        "execution_id": "exec-1",
        "remote": {"root": "s3://bucket/root"},
        "runnable": {"target": {"uri": "script"}, "kwargs": {}, "adapter": "dml-local-adapter", "sub": None},
        "state": None,
        "scratch_uri": "s3://bucket/root/scratch/exec-1",
        "cancel_requested_by": None,
    }

    response = state._call_adapter(envelope)

    assert response == {"lifecycle": "succeeded", "error": None, "state": None, "dag_id": "d" * 64}
    assert calls == [envelope]


def test_exec_state_subprocess_shim_preserves_resume_state(monkeypatch) -> None:
    state = _state()
    seen_states = []

    class PollingExecutor:
        @staticmethod
        def handle(**kwargs):
            seen_states.append(kwargs["state"])
            if kwargs["state"] is None:
                return {"lifecycle": "running", "error": None, "state": {"token": "abc"}, "dag_id": None}
            return {"lifecycle": "succeeded", "error": None, "state": None, "dag_id": "d" * 64}

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
        "cache_key": "cache",
        "execution_id": "exec-1",
        "remote": {"root": "s3://bucket/root"},
        "runnable": {"target": {"uri": "script"}, "kwargs": {}, "adapter": "dml-local-adapter", "sub": None},
        "scratch_uri": "s3://bucket/root/scratch/exec-1",
        "cancel_requested_by": None,
    }

    running = state._call_adapter({**base_envelope, "state": None})
    terminal = state._call_adapter({**base_envelope, "state": running["state"]})

    assert running == {"lifecycle": "running", "error": None, "state": {"token": "abc"}, "dag_id": None}
    assert terminal == {"lifecycle": "succeeded", "error": None, "state": None, "dag_id": "d" * 64}
    assert seen_states == [None, {"token": "abc"}]
