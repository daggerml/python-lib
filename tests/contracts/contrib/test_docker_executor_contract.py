from __future__ import annotations

import json
import os
import shutil
from typing import Any

import boto3
import pytest

from daggerml._internal.exec_state import ExecutionState
from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib import adapter_registry as areg
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib.adapters import AdapterBase, LocalAdapter
from daggerml.contrib.executors import DockerExecutor, ScriptExecutor


@pytest.fixture(autouse=True)
def _reset_registries(tmp_path, monkeypatch):
    areg._reset_for_tests()
    ereg._reset_for_tests()
    monkeypatch.setenv("DML_TEST_FN_STATE_DIR", str(tmp_path / "state"))
    # Ensure docker_bin resolves on platforms without docker (tests mock all docker calls)
    _orig_which = shutil.which
    monkeypatch.setattr(shutil, "which", lambda n: "/usr/bin/docker" if n == "docker" else _orig_which(n))
    areg.register_adapter(LocalAdapter)
    ereg.register_executor(ScriptExecutor)
    ereg.register_executor(DockerExecutor)
    yield
    areg._reset_for_tests()
    ereg._reset_for_tests()


def _remote() -> dict[str, str]:
    return {"root": os.environ["DML_REMOTE_URI"]}


def _sub_runnable() -> Runnable:
    return Runnable(target=Uri("script"), adapter="dml-local-adapter", kwargs={"x": 1}, sub=None)


def _docker_runnable(**kwargs: Any) -> Runnable:
    return Runnable(target=Uri("docker"), adapter="dml-local-adapter", kwargs=kwargs, sub=_sub_runnable())


def test_local_adapter_docker_resolve_runnable_shape():
    sub = _sub_runnable()
    result = LocalAdapter.resolve_runnable("docker", {"image": Uri("s3://bucket/image.tar"), "flags": ["--rm"]}, sub)

    assert isinstance(result, Runnable)
    assert result.target.uri == "docker"
    assert result.adapter == "dml-local-adapter"
    assert result.sub is sub
    assert result.kwargs == {"image": Uri("s3://bucket/image.tar"), "flags": ["--rm"]}


def test_local_adapter_docker_resolve_runnable_rejects_invalid_inputs():
    sub = _sub_runnable()

    with pytest.raises(DmlRepoError, match="requires sub runnable"):
        LocalAdapter.resolve_runnable("docker", {"image": Uri("s3://bucket/image.tar")}, None)

    with pytest.raises(DmlRepoError, match="requires image"):
        LocalAdapter.resolve_runnable("docker", {}, sub)


def test_docker_executor_start_launches_container_and_returns_running(monkeypatch):
    """start() should run docker and return launch state (no workdir/output_path in state)."""
    runnable = _docker_runnable(image="repo/name:tag", flags=["--rm"])
    docker_calls: list[tuple[Any, ...]] = []

    monkeypatch.setattr(DockerExecutor, "_prepare_image", staticmethod(lambda *args, **_: ("repo/name:tag", None)))
    monkeypatch.setattr(
        DockerExecutor,
        "_run_docker",
        staticmethod(lambda *args, **kwargs: docker_calls.append(args) or "cid-123"),
    )

    cache_key = "ck-docker-start"
    execution_id = "exec-docker-start"
    argv_ptr = "s3://test-bucket/argv"

    executor = DockerExecutor()
    result = executor.start(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        execution_id=execution_id,
        remote=_remote(),
    )

    assert result["status"] == "running"
    written_state = result["state"]
    assert written_state["container_id"] == "cid-123"
    # workdir and output_path must NOT be stored in state
    assert "workdir" not in written_state
    assert "output_path" not in written_state

    # Check docker run args
    run_args = docker_calls[0]
    assert run_args[0] == "run"
    assert "dml-local-adapter" in run_args
    assert "--poll" in run_args

    # input_uri and output_uri passed as S3 URIs
    run_args_str = " ".join(run_args)
    assert "s3://" in run_args_str


def test_docker_executor_start_writes_input_payload_to_s3(monkeypatch):
    """start() must write the sub-adapter payload to S3 via AdapterIO."""
    runnable = _docker_runnable(image="repo/name:tag")

    monkeypatch.setattr(DockerExecutor, "_prepare_image", staticmethod(lambda *args, **_: ("repo/name:tag", None)))
    monkeypatch.setattr(DockerExecutor, "_run_docker", staticmethod(lambda *args, **kwargs: "cid-payload"))

    cache_key = "ck-payload"
    execution_id = "exec-payload"
    remote = _remote()

    DockerExecutor().start(
        runnable=runnable,
        argv_ptr="s3://test-bucket/argv",
        cache_key=cache_key,
        execution_id=execution_id,
        remote=remote,
    )

    # Verify the input payload was written to S3 via AdapterIO
    exec_state = ExecutionState(cache_key, remote_root=remote["root"])
    io = exec_state.adapter_io(execution_id, "local:docker")
    raw = exec_state._get_object_bytes(io._input_key)
    assert raw is not None
    payload = json.loads(raw[0])
    assert payload["cache_key"] == cache_key
    assert payload["execution_id"] == execution_id
    assert payload["argv_ptr"] == "s3://test-bucket/argv"


def test_docker_executor_poll_returns_succeeded_when_container_exited_with_s3_result(monkeypatch):
    """poll() reads output from S3 via AdapterIO when container has exited."""
    import subprocess

    cache_key = "ck-poll-ok"
    execution_id = "exec-ok"
    remote = _remote()

    # Pre-write output to S3
    dag_id = "a" * 64
    exec_state = ExecutionState(cache_key, remote_root=remote["root"])
    io = exec_state.adapter_io(execution_id, "local:docker")
    exec_state._put_object(
        io._output_key,
        json.dumps({"status": "succeeded", "error": None, "dag_id": dag_id}).encode(),
    )

    job_state = {"container_id": "cid-ok", "cleanup_image": None}

    def fake_run(cmd, **kwargs):
        class FakeProc:
            returncode = 0
            stdout = "exited\n"
            stderr = ""
        return FakeProc()

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = DockerExecutor().poll(cache_key=cache_key, execution_id=execution_id, state=job_state, remote=remote)
    assert result["status"] == "succeeded"
    assert result["dag_id"] == dag_id


def test_docker_executor_poll_returns_running_when_container_still_running(monkeypatch):
    import subprocess

    job_state = {"container_id": "cid-running", "cleanup_image": None}

    def fake_run(cmd, **kwargs):
        class FakeProc:
            returncode = 0
            stdout = "running\n"
            stderr = ""
        return FakeProc()

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = DockerExecutor().poll(
        cache_key="ck-poll-running", execution_id="exec-running", state=job_state, remote=_remote()
    )
    assert result["status"] == "running"


def test_docker_executor_poll_returns_failed_when_no_s3_output(monkeypatch):
    import subprocess

    job_state = {"container_id": "cid-no-output", "cleanup_image": None}

    def fake_run(cmd, **kwargs):
        class FakeProc:
            returncode = 0
            stdout = "exited\n"
            stderr = ""
        return FakeProc()

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = DockerExecutor().poll(
        cache_key="ck-poll-no-output", execution_id="exec-no-output", state=job_state, remote=_remote()
    )
    assert result["status"] == "failed"
    assert "without output" in result["error"]


def test_docker_executor_cancel_removes_container_and_reports_cancelled(monkeypatch):
    cleanup_calls = []
    monkeypatch.setattr(shutil, "which", lambda name: "/usr/bin/docker" if name == "docker" else None)
    monkeypatch.setattr(
        "daggerml.contrib.executors.docker._cleanup_docker",
        lambda container_id, cleanup_image, docker_bin: cleanup_calls.append((container_id, cleanup_image, docker_bin)),
    )

    result = DockerExecutor().cancel(
        cache_key="ck-docker-cancel",
        execution_id="exec-docker-cancel",
        state={"container_id": "cid-cancel", "cleanup_image": "img:tmp"},
        remote=_remote(),
    )

    assert result == {"status": "cancelled", "error": None}
    assert cleanup_calls == [("cid-cancel", "img:tmp", "/usr/bin/docker")]


# ---------------------------------------------------------------------------
# AdapterBase._write_output S3 support
# ---------------------------------------------------------------------------


def test_write_output_writes_to_local_file(tmp_path):
    out = tmp_path / "result.json"
    AdapterBase._write_output(str(out), '{"status":"succeeded"}')
    assert out.read_text() == '{"status":"succeeded"}'


def test_write_output_writes_to_s3_uri():
    bucket = "test-bucket"
    key = "test-prefix/write-output-test.json"
    data = '{"status":"succeeded","dag_id":"' + "a" * 64 + '"}'
    AdapterBase._write_output(f"s3://{bucket}/{key}", data)

    s3 = boto3.client("s3")
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert body == data.encode("utf-8")


def test_write_output_s3_content_type_is_json():
    bucket = "test-bucket"
    key = "test-prefix/write-output-ct.json"
    AdapterBase._write_output(f"s3://{bucket}/{key}", '{}')

    head = boto3.client("s3").head_object(Bucket=bucket, Key=key)
    assert head["ContentType"] == "application/json"


def test_write_output_stdout(capsys):
    AdapterBase._write_output("-", '{"status":"running"}')
    captured = capsys.readouterr()
    assert '{"status":"running"}' in captured.out


def test_write_output_stdout_appends_newline_if_missing(capsys):
    AdapterBase._write_output("-", "no-newline")
    captured = capsys.readouterr()
    assert captured.out.endswith("\n")
