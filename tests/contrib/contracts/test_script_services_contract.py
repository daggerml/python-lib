import plistlib
import subprocess

import pytest

from daggerml.contrib.executors import _services as services


@pytest.mark.parametrize("platform,manager", [("linux", "systemd"), ("darwin", "launchd")])
def test_named_job_owns_supervisor_and_does_not_restart(monkeypatch, tmp_path, platform, manager):
    monkeypatch.setattr(services.sys, "platform", platform)
    calls = []
    monkeypatch.setattr(
        services, "_run", lambda *args: calls.append(args) or subprocess.CompletedProcess(args, 0, "", "")
    )
    handle = services.service_handle("exec", "s3://bucket/root")
    assert handle["manager"] == manager
    assert handle != services.service_handle("exec", "s3://bucket/other")
    services.start_service(handle, ["/python", "-m", "daggerml.contrib.supervisor"], tmp_path, {"KEY": "value"})
    if manager == "systemd":
        assert "--property=KillMode=control-group" in calls[0]
        assert "--property=Restart=no" in calls[0]
        assert "--property=SendSIGKILL=yes" in calls[0]
    else:
        job = plistlib.loads((tmp_path / "supervisor.plist").read_bytes())
        assert job["ProgramArguments"] == ["/python", "-m", "daggerml.contrib.supervisor"]
        assert job["KeepAlive"] is False
        assert job["AbandonProcessGroup"] is False
        assert job["LimitLoadToSessionType"] == "Background"
        assert job["EnvironmentVariables"] == {"KEY": "value"}
        assert calls[0][1] == "bootstrap"


@pytest.mark.parametrize(
    "state,active", [("active", True), ("deactivating", True), ("failed", False), ("inactive", False)]
)
def test_systemd_status_uses_manager_not_pids(monkeypatch, state, active):
    monkeypatch.setattr(services, "_run", lambda *args: subprocess.CompletedProcess(
        args, 0, f"LoadState=loaded\nActiveState={state}\nSubState=unused\n", ""
    ))
    assert services.service_active({"manager": "systemd", "name": "job.service"}) is active


@pytest.mark.parametrize("manager", ["systemd", "launchd"])
def test_manager_permission_errors_are_not_successful_teardown(monkeypatch, manager):
    monkeypatch.setattr(services, "_run", lambda *args: subprocess.CompletedProcess(args, 1, "", "Access denied"))
    with pytest.raises(services.DmlRepoError, match="Access denied"):
        services.stop_service({"manager": manager, "name": "job", "domain": "user/501"})


def test_launchd_does_not_complete_before_first_run(monkeypatch):
    handle = {"manager": "launchd", "name": "job", "domain": "user/501"}
    for runs, active in [(0, True), (1, False)]:
        monkeypatch.setattr(services, "_run", lambda *args, n=runs: subprocess.CompletedProcess(
            args, 0, f"state = not running\nruns = {n}\n", ""
        ))
        assert services.service_active(handle) is active
