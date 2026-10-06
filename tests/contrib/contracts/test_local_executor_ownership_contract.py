import json
from types import SimpleNamespace

import pytest

from daggerml.contrib.executors._ownership import _current_owner
from daggerml.contrib.executors.docker import DockerExecutor
from daggerml.contrib.executors.script import ScriptExecutor


@pytest.mark.parametrize("executor", [ScriptExecutor, DockerExecutor])
@pytest.mark.parametrize("operation", ["invoke", "cleanup", "cancel"])
@pytest.mark.parametrize("current_uid,current_host", [(1001, "host-A"), (1000, "host-B")])
def test_nonowner_never_accesses_local_resources(monkeypatch, executor, operation, current_uid, current_host):
    monkeypatch.setattr("daggerml.contrib.executors._ownership.os.geteuid", lambda: current_uid)
    monkeypatch.setattr("daggerml.contrib.executors._ownership.socket.gethostname", lambda: current_host)

    def forbidden(*args, **kwargs):
        pytest.fail("nonowner accessed a local resource")

    if executor is ScriptExecutor:
        for name in ("waitpid", "kill", "killpg"):
            monkeypatch.setattr(f"daggerml.contrib.executors.script.os.{name}", forbidden)
        monkeypatch.setattr("daggerml.contrib.executors.script.Path", forbidden)
        monkeypatch.setattr("daggerml.contrib.executors.script._cleanup_workdir", forbidden)
    else:
        monkeypatch.setattr("daggerml.contrib.executors.docker.shutil.which", forbidden)
        monkeypatch.setattr("daggerml.contrib.executors.docker.subprocess.run", forbidden)
    state = {"owner": "1000@host-A", "pid": 123, "container_id": "container", "workdir": "work", "extra": 1}
    payload = dict(operation=operation, cache_key="ck", execution_id="exec", runnable={},
                   remote={"root": "s3://bucket/root"}, scratch_uri="s3://bucket/scratch", adapter_state=state)
    if operation == "cleanup":
        payload["result_ref"] = "dag:result"
    elif operation == "cancel":
        payload.update(argv_ref="node-argv:argv", requested_by=None)
    assert executor.handle(**payload) == {"status": "retry", "error": None, "adapter_state": state}


def test_owner_uses_effective_uid_and_hostname(monkeypatch):
    monkeypatch.setattr("daggerml.contrib.executors._ownership.os.geteuid", lambda: 1000)
    monkeypatch.setattr("daggerml.contrib.executors._ownership.socket.gethostname", lambda: "host-A")
    monkeypatch.setenv("USER", "different-user")
    assert _current_owner() == "1000@host-A"
    assert _current_owner() == "1000@host-A"


@pytest.mark.parametrize("status,error", [("success", None), ("provider-error", "worker failed")])
@pytest.mark.parametrize("child_state", [False, True])
def test_docker_terminal_preserves_wrapper_state(monkeypatch, status, error, child_state):
    monkeypatch.setattr("daggerml.contrib.executors.docker._current_owner", lambda: "A")
    monkeypatch.setattr("daggerml.contrib.executors.docker.shutil.which", lambda _: "/docker")
    monkeypatch.setattr("daggerml.contrib.executors.docker.subprocess.run",
                        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="exited"))
    response = {"status": status, "error": error}
    if child_state:
        response["adapter_state"] = {"owner": "container", "pid": 123}
    monkeypatch.setattr("daggerml.contrib.executors.docker._read_scratch_output", lambda _: json.dumps(response))
    state = {"owner": "A", "container_id": "container", "cleanup_image": "image", "extra": 1}
    assert DockerExecutor().poll("ck", "exec", {}, state, {}, "s3://bucket/scratch") == {
        "status": status, "error": error, "state": state,
    }
