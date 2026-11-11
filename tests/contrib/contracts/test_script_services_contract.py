import plistlib
import stat
import subprocess
from types import SimpleNamespace

import pytest

from daggerml.contrib.executors import _services as services
from daggerml.contrib.executors import script
from daggerml.contrib.executors._ownership import _current_owner


@pytest.mark.parametrize("platform,tools,backend", [
    ("linux", {"systemd-run", "systemctl"}, "systemd"),
    ("linux", {"systemctl"}, "systemd"),
    ("linux", set(), None),
    ("darwin", {"launchctl"}, "launchd"),
    ("darwin", set(), None),
])
def test_backend_selection_depends_on_installed_tools(monkeypatch, platform, tools, backend):
    monkeypatch.setattr(services.sys, "platform", platform)
    monkeypatch.setattr(services.shutil, "which", lambda name: f"/bin/{name}" if name in tools else None)
    handle = services.select_service("exec", "s3://bucket/root")
    if backend is None:
        assert handle is None
    else:
        assert handle["backend"] == backend
        assert handle == services.select_service("exec", "s3://bucket/root")
        assert handle != services.select_service("other", "s3://bucket/root")
        assert handle != services.select_service("exec", "s3://bucket/other")


@pytest.mark.parametrize("backend", ["systemd", "launchd"])
def test_services_launch_without_waiting_for_completion(monkeypatch, tmp_path, backend):
    calls = []
    monkeypatch.setattr(services, "_run", lambda *args: calls.append(args)
                        or subprocess.CompletedProcess(args, 0, "", ""))
    handle = {"backend": backend, "unit": "daggerml-test.service",
              "label": "com.daggerml.exec.test", "domain": "user/1000"}
    command = ["/python", "-m", "daggerml.contrib.supervisor"]
    services.start_service(handle, command, tmp_path, {"KEY": "value"})
    if backend == "systemd":
        assert "--service-type=exec" in calls[0]
        for flag in ("--wait", "--pipe", "--pty", "--scope"):
            assert flag not in calls[0]
        assert "--property=Restart=no" in calls[0]
        assert "--property=KillMode=control-group" in calls[0]
        assert "--property=SendSIGKILL=yes" in calls[0]
    else:
        job = plistlib.loads((tmp_path / "supervisor.plist").read_bytes())
        assert job["ProgramArguments"] == command
        assert job["EnvironmentVariables"] == {"KEY": "value"}
        assert job["KeepAlive"] is False
        assert job["AbandonProcessGroup"] is False
        assert job["LimitLoadToSessionType"] == "Background"


@pytest.mark.parametrize("backend", ["systemd", "launchd"])
@pytest.mark.parametrize("failed", [False, True])
def test_script_service_launch_returns_handle_and_never_falls_back(monkeypatch, tmp_path, backend, failed):
    handle = {"backend": backend, "unit": "daggerml-test.service",
              "label": "com.daggerml.exec.test", "domain": "user/1000"}
    monkeypatch.setattr(script, "select_service", lambda *args: handle)
    monkeypatch.setattr(script.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path))

    def forbidden(*args, **kwargs):
        pytest.fail("service launch fell back to a process group or waited for completion")

    monkeypatch.setattr(script.subprocess, "Popen", forbidden)
    monkeypatch.setattr(script.subprocess, "run", forbidden)

    def start(saved, command, workdir, env):
        assert saved == handle
        assert workdir == tmp_path
        assert "daggerml.contrib.supervisor" in command
        assert env["DML_REMOTE_ROOT"] == "s3://bucket/root"
        if failed:
            raise services.DmlRepoError("manager failed")

    monkeypatch.setattr(script, "start_service", start)
    result = script.ScriptExecutor().start("ck", "exec", {}, {"root": "s3://bucket/root"}, "scratch")
    assert result["status"] == ("failure" if failed else "retry")
    assert result["state"]["handle"] == handle
    assert "pid" not in result["state"]
    assert "mode" not in result["state"]


@pytest.mark.parametrize("backend", ["systemd", "launchd", "pgroup"])
@pytest.mark.parametrize("operation", ["poll", "cleanup", "cancel"])
def test_continuations_use_only_the_saved_backend(monkeypatch, tmp_path, backend, operation):
    handle = {"backend": backend, "unit": "daggerml-test.service", "label": "com.daggerml.exec.test",
              "domain": f"user/{services.os.geteuid()}", "pgid": 123}
    state = {"owner": _current_owner(), "handle": handle, "workdir": str(tmp_path)}

    def forbidden(*args, **kwargs):
        pytest.fail("continuation redetected or switched backends")

    monkeypatch.setattr(script, "select_service", forbidden)
    monkeypatch.setattr(services.shutil, "which", forbidden)
    if backend == "pgroup":
        monkeypatch.setattr(script, "service_active", forbidden)
        monkeypatch.setattr(script, "stop_service", forbidden)
        monkeypatch.setattr(script.os, "waitpid", lambda *args: (0, 0))
        monkeypatch.setattr(script.os, "killpg", lambda *args: None)
    else:
        for name in ("waitpid", "kill", "killpg"):
            monkeypatch.setattr(script.os, name, forbidden)
        monkeypatch.setattr(script, "service_active", lambda saved: saved == handle)
        monkeypatch.setattr(script, "stop_service", lambda saved: saved == handle)
    args = ("ck", "exec", {}, state, {}, "scratch")
    executor = script.ScriptExecutor()
    result = getattr(executor, operation)(
        *args, *([] if operation == "poll" else ["dag:result" if operation == "cleanup" else None])
    )
    assert result["status"] == {"poll": "retry", "cleanup": "success", "cancel": "cancelled"}[operation]


@pytest.mark.parametrize("operation", ["poll", "cleanup", "cancel"])
def test_manager_failure_preserves_scratch_and_saved_handle(monkeypatch, tmp_path, operation):
    state = {"owner": _current_owner(), "handle": {"backend": "systemd", "unit": "daggerml-test.service"},
             "workdir": str(tmp_path)}

    def fail(*args):
        raise services.DmlRepoError("manager unavailable")

    monkeypatch.setattr(script, "service_active", fail)
    monkeypatch.setattr(script, "stop_service", fail)
    args = ("ck", "exec", {}, state, {}, "scratch")
    result = getattr(script.ScriptExecutor(), operation)(
        *args, *([] if operation == "poll" else ["dag:result" if operation == "cleanup" else None])
    )
    assert result["status"] == "failure"
    assert "manager unavailable" in result["error"]
    assert result["state"] == state
    assert tmp_path.exists()


@pytest.mark.parametrize("state,active", [("activating", True), ("active", True), ("deactivating", True),
                                         ("inactive", False), ("failed", False)])
def test_systemd_status(monkeypatch, state, active):
    monkeypatch.setattr(services, "_run", lambda *args: subprocess.CompletedProcess(
        args, 0, f"LoadState=loaded\nActiveState={state}\n", ""
    ))
    assert services.service_active({"backend": "systemd", "unit": "daggerml-test.service"}) is active


@pytest.mark.parametrize("runs,active", [(0, True), (1, False)])
def test_launchd_does_not_complete_before_first_run(monkeypatch, runs, active):
    monkeypatch.setattr(services, "_run", lambda *args: subprocess.CompletedProcess(
        args, 0, f"state = not running\nruns = {runs}\n", ""
    ))
    assert services.service_active({"backend": "launchd", "domain": "user/1000", "label": "test"}) is active


def test_systemd_teardown_stops_group_and_releases_retained_failure(monkeypatch):
    calls = []
    active = iter([True, False])
    monkeypatch.setattr(services, "service_active", lambda handle: next(active))
    monkeypatch.setattr(services, "_run", lambda *args: calls.append(args)
                        or subprocess.CompletedProcess(args, 0, "", ""))
    handle = {"backend": "systemd", "unit": "daggerml-test.service"}
    assert services.stop_service(handle)
    assert calls == [("systemctl", "--user", "stop", "--", handle["unit"]),
                     ("systemctl", "--user", "reset-failed", "--", handle["unit"])]


def test_systemd_teardown_retries_until_inactive(monkeypatch):
    monkeypatch.setattr(services, "service_active", lambda handle: True)
    monkeypatch.setattr(services, "_run", lambda *args: subprocess.CompletedProcess(args, 0, "", ""))
    assert not services.stop_service({"backend": "systemd", "unit": "daggerml-test.service"})


@pytest.mark.parametrize("operation", ["poll", "cleanup", "cancel"])
@pytest.mark.parametrize("handle", [None, {"backend": "unknown"}, {"backend": "pgroup", "pgid": 0},
                                   {"backend": "pgroup", "pgid": -1}, {"backend": "pgroup", "pgid": True}])
def test_invalid_handles_never_signal_processes(monkeypatch, tmp_path, operation, handle):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid handle accessed a process")

    for name in ("waitpid", "kill", "killpg"):
        monkeypatch.setattr(script.os, name, forbidden)
    state = {"owner": _current_owner(), "handle": handle, "workdir": str(tmp_path), "pid": 123}
    args = ("ck", "exec", {}, state, {}, "scratch")
    result = getattr(script.ScriptExecutor(), operation)(
        *args, *([] if operation == "poll" else ["dag:result" if operation == "cleanup" else None])
    )
    assert result["status"] == "failure"
    assert tmp_path.exists()


@pytest.mark.parametrize("runtime", [None, "/custom/runtime"])
def test_systemd_discovers_existing_user_bus(monkeypatch, runtime):
    monkeypatch.setattr(services.os, "geteuid", lambda: 1000)
    directory = runtime or "/run/user/1000"
    monkeypatch.setattr(services.Path, "stat", lambda path: SimpleNamespace(
        st_uid=1000, st_mode=stat.S_IFDIR if str(path) == directory else stat.S_IFSOCK
    ))
    env = {} if runtime is None else {"XDG_RUNTIME_DIR": runtime}
    original = dict(env)
    result = services._systemd_environment(env)
    assert result["XDG_RUNTIME_DIR"] == directory
    assert result["DBUS_SESSION_BUS_ADDRESS"] == f"unix:path={directory}/bus"
    assert env == original


def test_systemd_preserves_explicit_bus_configuration():
    env = {"DBUS_SESSION_BUS_ADDRESS": "unix:abstract=custom", "XDG_RUNTIME_DIR": "/custom"}
    assert services._systemd_environment(env) == env


@pytest.mark.parametrize("problem", ["missing", "owner", "type"])
def test_systemd_discovery_reports_unusable_bus(monkeypatch, problem):
    monkeypatch.setattr(services.os, "geteuid", lambda: 1000)

    def inspect(path):
        if problem == "missing":
            raise FileNotFoundError(str(path))
        return SimpleNamespace(st_uid=2000 if problem == "owner" else 1000,
                               st_mode=stat.S_IFREG if problem == "type" else stat.S_IFDIR)

    monkeypatch.setattr(services.Path, "stat", inspect)
    with pytest.raises(services.DmlRepoError, match="User systemd bus unavailable"):
        services._systemd_environment({})
