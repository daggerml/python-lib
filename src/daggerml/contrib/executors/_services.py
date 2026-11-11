"""Optional named OS services for nonblocking Script supervisor launches."""

import hashlib
import os
import plistlib
import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path

from daggerml.api import DmlRepoError


def select_service(execution_id: str, remote_root: str) -> dict[str, str] | None:
    """Select once by installed tools, not by whether a manager call succeeds."""
    identity = hashlib.sha256(f"{remote_root}\0{execution_id}".encode()).hexdigest()
    if sys.platform == "linux" and (shutil.which("systemd-run") or shutil.which("systemctl")):
        return {"backend": "systemd", "unit": f"daggerml-{identity}.service"}
    if sys.platform == "darwin" and shutil.which("launchctl"):
        return {"backend": "launchd", "label": f"com.daggerml.exec.{identity}", "domain": f"user/{os.geteuid()}"}
    return None


def _systemd_environment(env: dict[str, str]) -> dict[str, str]:
    """Connect noninteractive sessions to an existing user bus; never provision it."""
    env = dict(env)
    if env.get("DBUS_SESSION_BUS_ADDRESS"):
        return env
    uid = os.geteuid()
    runtime = Path(env.get("XDG_RUNTIME_DIR") or f"/run/user/{uid}")
    bus = runtime / "bus"
    try:
        directory, socket = runtime.stat(), bus.stat()
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


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "LC_ALL": "C"}
    if args[0] in {"systemctl", "systemd-run"}:
        env = _systemd_environment(env)
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=40, env=env)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise DmlRepoError(f"Script service manager unavailable: {exc}") from exc


def _check(result: subprocess.CompletedProcess[str]) -> None:
    if result.returncode:
        raise DmlRepoError(f"Script service manager failed: {result.stderr.strip()}")


def start_service(handle: dict, command: list[str], workdir: Path, env: dict[str, str]) -> None:
    if handle["backend"] == "systemd":
        _check(_run(
            "systemd-run", "--user", f"--unit={handle['unit']}", "--service-type=exec",
            "--property=Restart=no", "--property=KillMode=control-group",
            "--property=TimeoutStopSec=10s", "--property=SendSIGKILL=yes",
            f"--property=StandardOutput=append:{workdir / 'stdout.log'}",
            f"--property=StandardError=append:{workdir / 'stderr.log'}",
            *(f"--setenv={key}={value}" for key, value in env.items()), "--", *command,
        ))
    else:
        path = workdir / "supervisor.plist"
        path.write_bytes(plistlib.dumps({
            "Label": handle["label"], "ProgramArguments": command,
            "EnvironmentVariables": env, "RunAtLoad": True, "KeepAlive": False,
            "LimitLoadToSessionType": "Background",
            "AbandonProcessGroup": False, "ExitTimeOut": 10,
            "StandardOutPath": str(workdir / "stdout.log"),
            "StandardErrorPath": str(workdir / "stderr.log"),
        }))
        path.chmod(0o600)
        _check(_run("launchctl", "bootstrap", handle["domain"], str(path)))


def service_active(handle: dict) -> bool:
    if handle["backend"] == "systemd":
        result = _run("systemctl", "--user", "show", "--property=LoadState,ActiveState,SubState", "--", handle["unit"])
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
    result = _run("launchctl", "print", f"{handle['domain']}/{handle['label']}")
    if result.returncode and "Could not find service" in result.stderr:
        _check(_run("launchctl", "print", handle["domain"]))
        return None
    _check(result)
    return result.stdout


def stop_service(handle: dict) -> bool:
    """Confirm teardown before removing retained service state or scratch."""
    if handle["backend"] == "systemd":
        if service_active(handle):
            _check(_run("systemctl", "--user", "stop", "--", handle["unit"]))
        if service_active(handle):
            return False
        result = _run("systemctl", "--user", "reset-failed", "--", handle["unit"])
        if result.returncode and "not loaded" not in result.stderr and "not found" not in result.stderr:
            _check(result)
        return True
    result = _run("launchctl", "bootout", f"{handle['domain']}/{handle['label']}")
    if result.returncode:
        if _launchd_status(handle) is not None:
            _check(result)
        return True
    return _launchd_status(handle) is None
