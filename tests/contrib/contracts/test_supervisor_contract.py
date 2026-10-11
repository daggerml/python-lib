from __future__ import annotations

import io
import os
from pathlib import Path
from types import SimpleNamespace

import boto3
import pytest

import daggerml.contrib.supervisor as supervisor_mod
from daggerml import Ref


@pytest.mark.serial
@pytest.mark.parametrize("constructor_fails", [False, True])
def test_payload_environment_configures_supervisor_and_child(monkeypatch, tmp_path, constructor_fails):
    credentials = tmp_path / "credentials"
    credentials.write_text(
        "[ambient]\naws_access_key_id=ambient\naws_secret_access_key=ambient-secret\n"
        "[job]\naws_access_key_id=job\naws_secret_access_key=job-secret\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.setenv("AWS_PROFILE", "ambient")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("DML_REMOTE_ROOT", "s3://ambient/project")
    monkeypatch.setattr(boto3, "DEFAULT_SESSION", boto3.Session())
    original_session = boto3.DEFAULT_SESSION
    original_env = dict(os.environ)
    seen = {}

    def init(project_home, remote_root, user):
        seen["dml_env"] = dict(os.environ)
        assert remote_root == "s3://job/project"
        if constructor_fails:
            raise RuntimeError("constructor failed")
        return SimpleNamespace()

    class Sink:
        def __init__(self, **kwargs):
            client = supervisor_mod._create_logs_client()
            assert client.meta.region_name == "eu-west-1"
            assert client.meta.endpoint_url == "http://127.0.0.1:9"
            assert boto3.DEFAULT_SESSION.get_credentials().access_key == "job"
            client.close()
            seen["logs"] = True

        def close(self, **kwargs):
            pass

    class Proc:
        def __init__(self, cmd, **kwargs):
            seen["child_env"] = kwargs["env"]
            self.stdout = io.StringIO("")
            self.stderr = io.StringIO("")
            self.returncode = 0

        def poll(self):
            return 0

        def wait(self):
            return 0

    monkeypatch.setattr(supervisor_mod, "Dml", SimpleNamespace(init=init))
    monkeypatch.setattr(supervisor_mod, "_CloudWatchStream", Sink)
    monkeypatch.setattr(supervisor_mod.subprocess, "Popen", Proc)
    monkeypatch.setattr(supervisor_mod.tempfile, "mkdtemp", lambda prefix: str(tmp_path / "workdir"))
    payload = {
        "version": 0, "cache_key": "cache", "execution_id": "exec",
        "cmd": ["python", "worker.py"], "remote": {"root": "s3://job/project"},
        "env": {
            "DML_REMOTE_ROOT": "s3://conflicting/project", "DML_REMOTE_FETCH_WORKERS": "3",
            "AWS_REGION": "eu-west-1", "AWS_PROFILE": "job",
            "AWS_ENDPOINT_URL_CLOUDWATCH_LOGS": "http://127.0.0.1:9",
        },
    }
    if constructor_fails:
        with pytest.raises(RuntimeError, match="constructor failed"):
            supervisor_mod.run(payload)
    else:
        assert supervisor_mod.run(payload)["status"] == "failed"
        assert seen["logs"]
        assert seen["child_env"]["DML_REMOTE_ROOT"] == "s3://job/project"
        assert seen["child_env"]["AWS_PROFILE"] == "job"
    assert seen["dml_env"]["DML_REMOTE_ROOT"] == "s3://job/project"
    assert seen["dml_env"]["DML_REMOTE_FETCH_WORKERS"] == "3"
    assert seen["dml_env"]["AWS_PROFILE"] == "job"
    assert dict(os.environ) == original_env
    assert boto3.DEFAULT_SESSION is original_session


def test_run_uses_lifecycle_field_to_trigger_cancellation(monkeypatch, tmp_path: Path):
    calls: list[tuple[Ref, str | None]] = []

    class FakeRuntime:
        def read_execution_record(self, execution: Ref):
            assert execution == Ref("index:exec-1")
            return {"state": {"lifecycle": "cancel-pending"}}

        def cancel(self, execution: Ref, max_retries: int = 3):
            calls.append((execution, str(max_retries)))

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

    assert calls == []
    assert result["status"] == "failed"
    assert "Worker killed by signal SIGTERM" in result["error"]


def test_invalid_worker_result_includes_received_payload():
    result = {"status": "failed", "error": "docker build failed", "dag_id": None}

    with pytest.raises(supervisor_mod.DmlRepoError, match="received:") as exc_info:
        supervisor_mod._validate_output(result)

    assert repr(result) in str(exc_info.value)


@pytest.mark.parametrize(
    "result",
    [
        {"status": "succeeded", "error": None, "dag_id": "d" * 64},
        {"status": "failed", "error": "worker failed"},
    ],
)
def test_worker_terminal_schema_matches_script_output(result):
    assert supervisor_mod._validate_output(result) == result
