from __future__ import annotations

import json
import threading
from dataclasses import asdict
from types import SimpleNamespace

import pytest

from daggerml._core.db import Ref
from daggerml._core.types import Runnable, Uri
from daggerml.contrib.adapters import AdapterBase
from daggerml.contrib.executors.docker import DockerExecutor
from daggerml.contrib.executors.script import ScriptExecutor
from tests._core.contracts.test_execution_coordination import _record, _state


@pytest.mark.xfail(strict=True, reason="child registration can race parent cancellation")
def test_flaky_ci_001__registration_gap_leaves_existing_child_running(monkeypatch) -> None:
    """Pause between reverse-edge creation and the caller spawned summary write."""
    state = _state()
    assert state.create_execution_record(_record("parent", cache_key=None, argv_ref=None))
    assert state.create_execution_record(_record("child"))
    assert state._create_cache("cache", "child")
    edge_published = threading.Event()
    resume_registration = threading.Event()
    original_record_edge = state._record_edge

    def pause_after_edge(caller: str, callee: str) -> bool:
        created = original_record_edge(caller, callee)
        edge_published.set()
        assert resume_registration.wait(timeout=1)
        return created

    monkeypatch.setattr(state, "_record_edge", pause_after_edge)
    failures: list[BaseException] = []

    def register_child() -> None:
        try:
            state.get_or_start_fn(
                Ref("index:parent"), Runnable(Uri("target"), adapter="adapter"), Ref("node-argv:argv"), None
            )
        except BaseException as exc:
            failures.append(exc)

    worker = threading.Thread(target=register_child)
    worker.start()
    assert edge_published.wait(timeout=1)
    state.cancel("parent", "user", None)
    resume_registration.set()
    worker.join(timeout=1)

    assert failures
    assert state.read_execution_record("child")["state"]["lifecycle"] == "canceled"


@pytest.mark.xfail(strict=True, reason="docker inspect transport failures are treated as exited containers")
def test_flaky_ci_003__docker_inspect_error_is_not_a_terminal_container_exit(monkeypatch) -> None:
    monkeypatch.setattr("daggerml.contrib.executors.docker.shutil.which", lambda _: "/docker")
    monkeypatch.setattr(
        "daggerml.contrib.executors.docker.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout="", stderr="daemon unavailable"),
    )
    monkeypatch.setattr("daggerml.contrib.executors.docker._read_scratch_output", lambda _: None)

    result = DockerExecutor().poll(
        cache_key="cache",
        execution_id="exec",
        runnable={},
        state={"container_id": "container"},
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/root/exec/io/exec",
    )

    assert result["status"] == "retry"


@pytest.mark.xfail(strict=True, reason="nested cleanup errors escape before output.json is published")
def test_flaky_ci_004__nested_cleanup_failure_still_publishes_diagnostics(tmp_path, monkeypatch) -> None:
    class NestedAdapter(AdapterBase):
        @classmethod
        def send(cls, **kwargs):
            if kwargs["operation"] == "cleanup":
                raise RuntimeError("cleanup failed")
            return {"status": "success", "error": None, "adapter_state": {}}

    class State:
        def read_execution_record(self, execution_id):
            return {"state": {"result_ref": "dag:result"}}

    payload = {
        "operation": "invoke",
        "cache_key": "cache",
        "execution_id": "exec",
        "remote": {"root": "s3://bucket/root"},
        "runnable": asdict(Runnable(Uri("target"), adapter="adapter")),
        "scratch_uri": "s3://bucket/root/exec/io/exec",
        "adapter_state": None,
    }
    input_path, output_path = tmp_path / "input.json", tmp_path / "output.json"
    input_path.write_text(json.dumps(payload))
    monkeypatch.setattr("daggerml.contrib.adapters.ExecutionState.from_execution_id", lambda *_args, **_kwargs: State())

    assert NestedAdapter.cli(["--poll", "-i", str(input_path), "-o", str(output_path)]) == 0
    assert "cleanup failed" in json.loads(output_path.read_text())["error"]


@pytest.mark.xfail(strict=True, reason="fresh runtime success returns before outer executor cleanup")
def test_flaky_ci_005__fresh_success_drives_outer_cleanup(monkeypatch) -> None:
    state = _state()
    assert state.create_execution_record(_record("caller", cache_key=None, argv_ref=None))
    assert state.create_execution_record(_record("child"))
    assert state._create_cache("cache", "child")
    calls: list[str] = []

    def call_adapter(request):
        calls.append(request["operation"])
        if request["operation"] == "invoke":
            state.finish_execution("child", Ref("dag:result"), None)
        return {"status": "success"}

    monkeypatch.setattr(state, "_call_adapter", call_adapter)

    assert state.get_or_start_fn(
        Ref("index:caller"), Runnable(Uri("target"), adapter="adapter"), Ref("node-argv:argv"), None
    ) == Ref("dag:result")
    assert calls == ["invoke", "cleanup"]


@pytest.mark.xfail(strict=True, reason="script cancellation acknowledges before process teardown")
def test_flaky_ci_006__script_cancel_waits_for_terminated_process_group(monkeypatch, tmp_path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    monkeypatch.setattr("daggerml.contrib.executors.script.os.killpg", lambda *_: None)
    monkeypatch.setattr("daggerml.contrib.executors.script.os.waitpid", lambda *_: (0, 0))

    result = ScriptExecutor().cancel(
        cache_key="cache",
        execution_id="exec",
        runnable={},
        state={"pid": 123, "workdir": str(workdir)},
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/root/exec/io/exec",
        cancel_requested_by="user",
    )

    assert result["status"] == "retry"
