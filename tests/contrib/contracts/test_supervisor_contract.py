from __future__ import annotations

import io
from pathlib import Path
from types import SimpleNamespace

import pytest

import daggerml.contrib.supervisor as supervisor_mod
from daggerml import Ref


def test_contrib_supervisor_001__run_uses_lifecycle_field_to_trigger_cancellation(monkeypatch, tmp_path: Path):
    calls: list[tuple[Ref, str | None]] = []

    class FakeRuntime:
        def read_execution_record(self, execution: Ref):
            assert execution == Ref("index:exec-1")
            return {"lifecycle": "cancel-pending"}

        def cancel(self, execution: Ref, mode: str = "full"):
            calls.append((execution, mode))

    class FakeDml:
        @staticmethod
        def init(project_home: str, remote_root: str, user: str):
            assert project_home.endswith("repo")
            assert remote_root == "s3://bucket/root"
            assert user == "worker"
            return SimpleNamespace(runtime=FakeRuntime())

    class FakeSink:
        def __init__(self, *, cache_key: str, execution_id: str, stream_kind: str):
            self.events: list[tuple[str, str | None]] = []

        def emit_lifecycle(self, *, event: str, terminal_status: str | None = None) -> None:
            self.events.append((event, terminal_status))

        def emit(self, message: str) -> None:
            return None

        def close(self, *, terminal_status: str) -> None:
            self.events.append(("close", terminal_status))

    class FakeProc:
        def __init__(self, *args, **kwargs):
            self.stdout = io.StringIO("")
            self.stderr = io.StringIO("")
            self.returncode = None
            self.terminated = False

        def poll(self):
            return None if not self.terminated else self.returncode

        def terminate(self):
            self.terminated = True
            self.returncode = -15

        def wait(self):
            self.terminated = True
            self.returncode = -15
            return self.returncode

    monkeypatch.setattr(supervisor_mod, "Dml", FakeDml)
    monkeypatch.setattr(supervisor_mod, "_CloudWatchStream", FakeSink)
    monkeypatch.setattr(supervisor_mod.subprocess, "Popen", FakeProc)
    monkeypatch.setattr(supervisor_mod.tempfile, "mkdtemp", lambda prefix: str(tmp_path / "workdir"))

    result = supervisor_mod.run(
        {
            "version": 0,
            "cache_key": "cache-1",
            "execution_id": "exec-1",
            "cmd": ["python", "worker.py"],
            "remote": {"root": "s3://bucket/root"},
            "env": {},
        }
    )

    assert calls == [(Ref("index:exec-1"), "drive")]
    assert result["status"] == "failed"
    assert "Worker killed by signal SIGTERM" in result["error"]


def test_contrib_supervisor_002__invalid_worker_result_includes_received_payload():
    result = {"status": "failed", "error": "docker build failed", "dag_id": None}

    with pytest.raises(supervisor_mod.DmlRepoError, match="received:") as exc_info:
        supervisor_mod._validate_output(result)

    assert repr(result) in str(exc_info.value)
