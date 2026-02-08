from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, cast

from daggerml._internal import DmlOps
from daggerml._internal.types import DmlRepoError
from daggerml.contrib.executor_state import Status, lock_from_comms

LEASE_SECONDS = 30.0


def _parse_cmd_payload(
    payload: dict[str, Any],
) -> tuple[str, list[str], dict[str, str], dict[str, Any], dict[str, Any]]:
    allowed = {"version", "cache_key", "cmd", "remote", "comms", "env"}
    unknown = sorted(set(payload.keys()) - allowed)
    if unknown:
        bad = ", ".join(unknown)
        raise DmlRepoError(f"Supervisor payload has unknown fields: {bad}")

    version = payload.get("version")
    if version != 1:
        raise DmlRepoError("Supervisor payload version must be 1")

    cache_key = payload.get("cache_key")
    if not isinstance(cache_key, str) or not cache_key:
        raise DmlRepoError("Supervisor payload cache_key must be a non-empty string")

    cmd = payload.get("cmd")
    if not isinstance(cmd, list) or not cmd or not all(isinstance(x, str) and x for x in cmd):
        raise DmlRepoError("Supervisor payload cmd must be a non-empty list[str]")

    remote = payload.get("remote")
    if not isinstance(remote, dict):
        raise DmlRepoError("Supervisor payload remote must be a dict")
    if not isinstance(remote.get("root"), str) or not isinstance(remote.get("cache"), str):
        raise DmlRepoError("Supervisor payload remote requires string root/cache")

    comms = payload.get("comms")
    if not isinstance(comms, dict):
        raise DmlRepoError("Supervisor payload comms must be a dict")
    if not isinstance(comms.get("kind"), str) or not isinstance(comms.get("spec"), dict):
        raise DmlRepoError("Supervisor payload comms requires kind/spec")

    env = payload.get("env") or {}
    if not isinstance(env, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()):
        raise DmlRepoError("Supervisor payload env must be a dict[str,str]")

    merged_env = os.environ.copy()
    merged_env.update(env)
    return cache_key, cmd, merged_env, remote, comms


def _launch(payload: dict[str, Any]) -> dict[str, Any]:
    cache_key, cmd, env, remote, comms = _parse_cmd_payload(payload)
    workdir = tempfile.mkdtemp(prefix=f"dml-supervisor-{cache_key[:8]}-")
    repo_dir = Path(workdir) / "repo"
    with DmlOps.create(str(repo_dir), user="worker"):
        pass
    env = dict(env)
    env["DML_REMOTE_ROOT"] = remote["root"]
    env["DML_REMOTE_CACHE"] = remote["cache"]
    env["DML_REPO"] = str(repo_dir)
    if comms.get("kind") == "local":
        spec = comms.get("spec")
        if isinstance(spec, dict):
            cache_dir = spec.get("cache_dir", spec.get("root_dir"))
            if isinstance(cache_dir, str) and cache_dir:
                env["DML_FN_CACHE_DIR"] = cache_dir
    stdout_path = Path(workdir) / "stdout.log"
    stderr_path = Path(workdir) / "stderr.log"
    with stdout_path.open("w") as stdout_f, stderr_path.open("w") as stderr_f:
        proc = subprocess.Popen(
            cmd,
            cwd=workdir,
            env=env,
            stdout=stdout_f,
            stderr=stderr_f,
            start_new_session=False,
            close_fds=True,
        )
    return {
        "status": "running",
        "error": None,
        "pid": proc.pid,
        "workdir": workdir,
        "repo_dir": str(repo_dir),
        "result_path": str(Path(workdir) / "result.json"),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
    }


def _validate_output(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        raise DmlRepoError("Supervisor result must be a dict")
    expected = {"status", "error"}
    if set(result.keys()) != expected:
        raise DmlRepoError("Supervisor result keys must be exactly: status, error")
    status = result.get("status")
    if status not in {"pending", "running", "succeeded", "failed", "canceled"}:
        raise DmlRepoError("Supervisor result status must be one of pending|running|succeeded|failed|canceled")
    error = result.get("error")
    if status == "failed":
        if error is None:
            raise DmlRepoError("Supervisor result failed requires error")
    else:
        if error is not None:
            raise DmlRepoError("Supervisor result running/pending/succeeded/canceled requires error=None")
    return result


def run(payload: dict[str, Any], *, heartbeat_s: float = 0.25) -> dict[str, Any]:
    launched = _launch(payload)
    cache_key = payload["cache_key"]
    comms = payload["comms"]
    owner_instance = f"supervisor:{os.getpid()}"
    pid = launched["pid"]
    result_path = Path(launched["result_path"])
    while True:
        try:
            done_pid, status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            done_pid, status = pid, 0
        if done_pid == 0:
            now = time.time()
            with lock_from_comms(cache_key, comms) as state:
                if state is not None:
                    existing = state.get()
                    owner_executor = existing.get("owner_executor") if isinstance(existing, dict) else None
                    running = state.update_status(
                        status="running",
                        error=None,
                        owner_executor=owner_executor,
                        owner_instance=owner_instance,
                        heartbeat_ts=now,
                        lease_expires_ts=now + LEASE_SECONDS,
                    )
                    state.update(running)
            time.sleep(heartbeat_s)
            continue
        if result_path.exists():
            try:
                parsed = json.loads(result_path.read_text())
                terminal = _validate_output(parsed)
            except Exception as e:
                terminal = {"status": "failed", "error": f"Supervisor could not read worker result: {e}"}
        elif os.WIFSIGNALED(status):
            terminal = {"status": "failed", "error": f"Worker exited on signal {os.WTERMSIG(status)}"}
        else:
            code = os.WEXITSTATUS(status) if os.WIFEXITED(status) else -1
            terminal = {"status": "failed", "error": f"Worker exited without result (code={code})"}

        with lock_from_comms(cache_key, comms) as state:
            if state is not None:
                existing = state.get()
                owner_executor = existing.get("owner_executor") if isinstance(existing, dict) else None
                finished = state.update_status(
                    status=cast(Status, terminal["status"]),
                    error=terminal["error"],
                    owner_executor=owner_executor,
                    owner_instance=owner_instance,
                    heartbeat_ts=time.time(),
                    lease_expires_ts=None,
                )
                state.update(finished)
        return terminal


def _read(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    return Path(path).read_text()


def _write(path: str, data: str) -> None:
    if path == "-":
        sys.stdout.write(data)
        if not data.endswith("\n"):
            sys.stdout.write("\n")
        sys.stdout.flush()
        return
    Path(path).write_text(data)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="daggerml contrib supervisor")
    parser.add_argument("-i", "--input", default="-")
    parser.add_argument("-o", "--output", default="-")
    args = parser.parse_args(argv or sys.argv[1:])
    payload = json.loads(_read(args.input))
    result = run(payload)
    _write(args.output, json.dumps(result, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
