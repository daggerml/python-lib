"""Named, non-restarting OS jobs for Script supervisors; no detached PID fallback."""

import hashlib
import os
import plistlib
import re
import stat
import subprocess
import sys
from pathlib import Path

from daggerml.api import DmlRepoError


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "LC_ALL": "C"}
    if args[0] in {"systemctl", "systemd-run"}:
        env = _systemd_environment(env)
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=40,
                              env=env)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise DmlRepoError(f"Script service manager unavailable: {exc}") from exc


def _systemd_environment(env: dict[str, str]) -> dict[str, str]:
    """Find the existing user bus for noninteractive sessions; never provision it."""
    env = dict(env)
    if env.get("DBUS_SESSION_BUS_ADDRESS"):
        return env
    uid = os.geteuid()
    runtime = Path(env.get("XDG_RUNTIME_DIR") or f"/run/user/{uid}")
    bus = runtime / "bus"
    try:
        directory = runtime.stat()
        socket = bus.stat()
        if (not stat.S_ISDIR(directory.st_mode) or directory.st_uid != uid
                or not stat.S_ISSOCK(socket.st_mode) or socket.st_uid != uid):
            raise ValueError("runtime directory and bus socket must belong to the effective user")
    except (OSError, ValueError) as exc:
        raise DmlRepoError(
            f"User systemd bus unavailable at {bus}: {exc}. "
            "Configure a running user systemd manager and user bus, or set DBUS_SESSION_BUS_ADDRESS explicitly."
        ) from exc
    env["XDG_RUNTIME_DIR"] = str(runtime)
    env["DBUS_SESSION_BUS_ADDRESS"] = f"unix:path={bus}"
    return env


def _check(result: subprocess.CompletedProcess[str]) -> None:
    if result.returncode:
        raise DmlRepoError(f"Script service manager failed: {result.stderr.strip()}")


def service_handle(execution_id: str, remote_root: str) -> dict[str, str]:
    identity = hashlib.sha256(f"{remote_root}\0{execution_id}".encode()).hexdigest()
    if sys.platform == "linux":
        return {"manager": "systemd", "name": f"daggerml-{identity}.service"}
    if sys.platform == "darwin":
        return {"manager": "launchd", "name": f"com.daggerml.exec.{identity}", "domain": f"user/{os.geteuid()}"}
    raise DmlRepoError("Script execution requires systemd (Linux) or launchd (macOS)")


def start_service(handle: dict, command: list[str], workdir: Path, env: dict[str, str]) -> None:
    if handle["manager"] == "systemd":
        _check(_run(
            "systemd-run", "--user", f"--unit={handle['name']}", "--service-type=exec",
            "--property=Restart=no", "--property=KillMode=control-group",
            "--property=TimeoutStopSec=10s", "--property=SendSIGKILL=yes",
            f"--property=StandardOutput=append:{workdir / 'stdout.log'}",
            f"--property=StandardError=append:{workdir / 'stderr.log'}",
            *(f"--setenv={key}={value}" for key, value in env.items()), "--", *command,
        ))
    else:
        path = workdir / "supervisor.plist"
        path.write_bytes(plistlib.dumps({
            "Label": handle["name"], "ProgramArguments": command,
            "EnvironmentVariables": env, "RunAtLoad": True, "KeepAlive": False,
            "LimitLoadToSessionType": "Background",
            "AbandonProcessGroup": False, "ExitTimeOut": 10,
            "StandardOutPath": str(workdir / "stdout.log"),
            "StandardErrorPath": str(workdir / "stderr.log"),
        }))
        path.chmod(0o600)
        _check(_run("launchctl", "bootstrap", handle["domain"], str(path)))


def service_active(handle: dict) -> bool:
    if handle["manager"] == "systemd":
        result = _run("systemctl", "--user", "show", "--property=LoadState,ActiveState,SubState", "--", handle["name"])
        properties = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        if properties.get("LoadState") == "not-found":
            return False
        _check(result)
        state = properties.get("ActiveState")
        if state in {"inactive", "failed"}:
            return False
        if state in {"active", "activating", "deactivating", "reloading", "refreshing"}:
            return True
        raise DmlRepoError(f"Unrecognized Script service state: {state!r}")
    output = _launchd_status(handle)
    if output is None:
        return False
    state = re.search(r"^\s*state = (.+)$", output, re.MULTILINE)
    runs = re.search(r"^\s*runs = (\d+)$", output, re.MULTILINE)
    if state is None:
        raise DmlRepoError("Unrecognized launchd Script job status")
    return not (state[1] == "not running" and runs is not None and int(runs[1]) > 0)


def _launchd_status(handle: dict) -> str | None:
    result = _run("launchctl", "print", f"{handle['domain']}/{handle['name']}")
    if result.returncode and "Could not find service" in result.stderr:
        # A missing job is not the same as an unavailable user domain.
        _check(_run("launchctl", "print", handle["domain"]))
        return None
    _check(result)
    return result.stdout


def stop_service(handle: dict) -> bool:
    """Return true only after manager-confirmed teardown, including retained job removal."""
    if handle["manager"] == "systemd":
        if service_active(handle):
            _check(_run("systemctl", "--user", "stop", "--", handle["name"]))
        if service_active(handle):
            return False
        # Successful transient units unload automatically; release retained failures.
        result = _run("systemctl", "--user", "reset-failed", "--", handle["name"])
        if result.returncode and "not loaded" not in result.stderr and "not found" not in result.stderr:
            _check(result)
        return True
    target = f"{handle['domain']}/{handle['name']}"
    result = _run("launchctl", "bootout", target)
    if result.returncode:
        # Ignore only confirmed absence; permissions/manager failures remain failures.
        if _launchd_status(handle) is not None:
            _check(result)
        return True
    return _launchd_status(handle) is None
