from __future__ import annotations

import json
from types import SimpleNamespace

from daggerml.contrib.executors.docker import DockerExecutor
from daggerml.contrib.executors.lambda_ import LambdaExecutorBase
from daggerml.contrib.executors.script import ScriptExecutor


def test_docker_start_uses_execution_id_temp_prefix_and_adapter_state(tmp_path, monkeypatch) -> None:
    prefixes = []
    payloads = []
    workdir = tmp_path / "docker-work"

    def mkdtemp(*, prefix):
        prefixes.append(prefix)
        workdir.mkdir()
        return str(workdir)

    monkeypatch.setattr("daggerml.contrib.executors.docker.tempfile.mkdtemp", mkdtemp)
    monkeypatch.setattr(
        DockerExecutor, "_prepare_image", staticmethod(lambda runnable, path, remote: ("image", None))
    )
    monkeypatch.setattr(DockerExecutor, "_run_docker", staticmethod(lambda *args, **kwargs: "container-1"))
    monkeypatch.setattr(
        "daggerml.contrib.executors.docker._write_scratch_json",
        lambda uri, payload, raw: payloads.append(json.loads(payload)),
    )

    DockerExecutor().start(
        cache_key="ck",
        execution_id="exec-123",
        runnable={"sub": {"adapter": "nested"}, "kwargs": {"image": "image"}},
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/root/exec/io/exec-123/",
    )

    assert prefixes == ["dml-docker-exec-123-"]
    assert payloads[0]["adapter_state"] is None
    assert "state" not in payloads[0]


def test_script_start_uses_execution_id_temp_prefix(tmp_path, monkeypatch) -> None:
    prefixes = []
    workdir = tmp_path / "script-work"

    def mkdtemp(*, prefix):
        prefixes.append(prefix)
        workdir.mkdir()
        return str(workdir)

    monkeypatch.setattr("daggerml.contrib.executors.script.tempfile.mkdtemp", mkdtemp)
    monkeypatch.setattr(
        "daggerml.contrib.executors.script.subprocess.Popen",
        lambda *args, **kwargs: SimpleNamespace(pid=123),
    )

    ScriptExecutor().start(
        cache_key="ck",
        execution_id="exec-123456789",
        runnable={},
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/root/exec/io/exec-123456789/",
    )

    assert prefixes == ["dml-script-exec-123456789-"]


def test_docker_cancel_preserves_adapter_state(monkeypatch) -> None:
    monkeypatch.setattr("daggerml.contrib.executors.docker.shutil.which", lambda command: None)

    result = DockerExecutor().cancel(
        cache_key="ck",
        execution_id="exec",
        runnable={},
        state={"container_id": "container-1"},
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/root/exec/io/exec/",
        cancel_requested_by="user",
        argv_ptr="node-argv:abc",
    )

    assert result["state"] == {"container_id": "container-1"}
    assert result["status"] == "failure"


def test_docker_cleanup_retries_active_then_prunes_idempotently(monkeypatch) -> None:
    statuses = iter(("running", "exited", "missing"))
    cleanup_calls = []
    monkeypatch.setattr("daggerml.contrib.executors.docker.shutil.which", lambda command: "/docker")
    monkeypatch.setattr(DockerExecutor, "_run_docker", staticmethod(lambda *args, **kwargs: next(statuses)))
    monkeypatch.setattr(
        "daggerml.contrib.executors.docker._cleanup_docker",
        lambda container_id, image, docker_bin: cleanup_calls.append((container_id, image, docker_bin)),
    )
    executor = DockerExecutor()
    kwargs = {
        "cache_key": "ck",
        "execution_id": "exec",
        "runnable": {},
        "state": {"container_id": "container-1", "cleanup_image": "image-1"},
        "remote": {"root": "s3://bucket/root"},
        "scratch_uri": "s3://bucket/root/exec/io/exec/",
        "result_ref": "dag:result",
    }

    assert executor.cleanup(**kwargs)["status"] == "retry"
    assert executor.cleanup(**kwargs)["status"] == "success"
    assert executor.cleanup(**kwargs)["status"] == "success"
    assert cleanup_calls == [
        ("container-1", "image-1", "/docker"),
        ("container-1", "image-1", "/docker"),
    ]


def test_lambda_handler_failure_returns_object_adapter_state() -> None:
    class BrokenExecutor(LambdaExecutorBase):
        def start(self, **kwargs):
            raise RuntimeError("boom")

        def poll(self, **kwargs):
            raise RuntimeError("boom")

    result = BrokenExecutor.handler(
        {
            "operation": "invoke",
            "cache_key": "ck",
            "execution_id": "exec",
            "remote": {"root": "s3://bucket/root"},
            "runnable": {},
            "adapter_state": {"attempt": 1},
            "scratch_uri": "s3://bucket/root/exec/io/exec/",
        },
        None,
    )

    assert result["status"] == "failure"
    assert result["adapter_state"] == {"attempt": 1}
    assert "boom" in result["error"]
