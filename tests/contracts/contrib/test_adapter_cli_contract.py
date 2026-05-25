from __future__ import annotations

import json

from daggerml._internal.exec_state import ExecutionState
from daggerml._internal.types import Runnable, Uri
from daggerml.contrib.adapters import AdapterBase


def test_adapter_cli_poll_preserves_launch_state_over_execution_record_state(monkeypatch, capsys):
    seen_states = []

    class DummyAdapter(AdapterBase):
        @classmethod
        def send(
            cls, *, runnable, argv_ptr, cache_key, execution_id, remote, state, execution_status, cancel_requested_by
        ):
            del runnable, argv_ptr, cache_key, execution_id, remote, execution_status, cancel_requested_by
            seen_states.append(state)
            if len(seen_states) == 1:
                return {"status": "running", "error": None, "state": {"result_path": "/tmp/result.json"}}
            return {"status": "succeeded", "error": None, "dag_id": "a" * 64}

    monkeypatch.setattr(
        ExecutionState,
        "read_launch_state",
        lambda self, execution_id: {
            "resume_state": {"container_id": "cid-123"},
            "created_at": 1,
            "execution_id": execution_id,
            "cache_key": self.cache_key,
        },
    )
    monkeypatch.setattr(
        ExecutionState,
        "read_execution_record",
        lambda self, execution_id: {
            "execution_id": execution_id,
            "cache_key": self.cache_key,
            "lifecycle": "running",
            "updated_at": 1,
            "spawned_execution_ids": [],
            "cancellation_requested_by": None,
        },
    )
    monkeypatch.setattr("daggerml.contrib.adapters.time.sleep", lambda _: None)
    monkeypatch.setattr(
        "sys.stdin.read",
        lambda: DummyAdapter._dump_payload(
            runnable=Runnable(target=Uri("dummy"), adapter="dummy", kwargs={}),
            argv_ptr="ptr",
            cache_key="ck",
            execution_id="exec-ck",
            remote={"root": "s3://bucket/root"},
            state=None,
        ),
    )

    exit_code = DummyAdapter.cli(["--poll"])

    assert exit_code == 0
    assert seen_states == [None, {"result_path": "/tmp/result.json"}]
    assert json.loads(capsys.readouterr().out.strip()) == {"status": "succeeded", "error": None, "dag_id": "a" * 64}
