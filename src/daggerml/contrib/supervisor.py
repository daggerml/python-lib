from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from daggerml._internal import DmlOps
from daggerml._internal.types import DmlRepoError


def _parse_cmd_payload(
    payload: dict[str, Any],
) -> tuple[str, str, list[str], dict[str, str], dict[str, str]]:
    allowed = {"version", "cache_key", "execution_id", "cmd", "remote", "env"}
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise DmlRepoError(f"Supervisor payload has unknown fields: {', '.join(unknown)}")

    version = payload.get("version")
    if version != 2:
        raise DmlRepoError("Supervisor payload version must be 2")

    cache_key = payload.get("cache_key")
    if not isinstance(cache_key, str) or not cache_key:
        raise DmlRepoError("Supervisor payload cache_key must be a non-empty string")

    execution_id = payload.get("execution_id")
    if not isinstance(execution_id, str) or not execution_id:
        raise DmlRepoError("Supervisor payload execution_id must be a non-empty string")

    cmd = payload.get("cmd")
    if not isinstance(cmd, list) or not cmd or not all(isinstance(x, str) and x for x in cmd):
        raise DmlRepoError("Supervisor payload cmd must be a non-empty list[str]")

    remote = payload.get("remote")
    if not isinstance(remote, dict):
        raise DmlRepoError("Supervisor payload remote must be a dict")
    unknown_remote = sorted(set(remote) - {"root"})
    if unknown_remote:
        raise DmlRepoError(f"Supervisor payload remote has unknown fields: {', '.join(unknown_remote)}")
    if not isinstance(remote.get("root"), str):
        raise DmlRepoError("Supervisor payload remote requires string root")

    env = payload.get("env") or {}
    if not isinstance(env, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()):
        raise DmlRepoError("Supervisor payload env must be a dict[str,str]")

    merged_env = os.environ.copy()
    merged_env.update(env)
    return cache_key, execution_id, cmd, merged_env, {"root": remote["root"]}


def _validate_output(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        raise DmlRepoError("Supervisor result must be a dict")
    status = result.get("status")
    if status not in {"succeeded", "failed"}:
        raise DmlRepoError("Supervisor result status must be one of succeeded|failed after worker exit")
    if status == "failed":
        expected = {"status", "error"}
        if set(result.keys()) != expected:
            raise DmlRepoError("Supervisor failed result keys must be exactly: status, error")
        error = result.get("error")
        if error is None:
            raise DmlRepoError("Supervisor result failed requires error")
        return result

    expected = {"status", "error", "dag_id"}
    if set(result.keys()) != expected:
        raise DmlRepoError("Supervisor succeeded result keys must be exactly: status, error, dag_id")
    error = result.get("error")
    if error is not None:
        raise DmlRepoError("Supervisor result succeeded requires error=None")
    dag_id = result.get("dag_id")
    if not isinstance(dag_id, str) or not re.fullmatch(r"[0-9a-f]{64}", dag_id):
        raise DmlRepoError("Supervisor result succeeded requires real dag_id")
    return result


def run(payload: dict[str, Any]) -> dict[str, Any]:
    """Launch a worker subprocess, wait for it to exit, and return the terminal result."""
    cache_key, execution_id, cmd, env, remote = _parse_cmd_payload(payload)
    workdir = tempfile.mkdtemp(prefix=f"dml-supervisor-{execution_id[:8]}-")
    repo_dir = Path(workdir) / "repo"
    with DmlOps.create(str(repo_dir), user="worker", remote_root=remote["root"]):
        pass
    env = dict(env)
    env["DML_PROJECT_HOME"] = str(repo_dir)
    result_path = Path(workdir) / "result.json"
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
    # Wait synchronously for the worker to finish
    proc.wait()

    if result_path.exists():
        try:
            parsed = json.loads(result_path.read_text())
            return _validate_output(parsed)
        except Exception as e:
            return {"status": "failed", "error": f"Supervisor could not read worker result: {e}"}

    if proc.returncode is not None and proc.returncode < 0:
        import signal as _signal
        sig = -proc.returncode
        try:
            sig_name = _signal.Signals(sig).name
        except ValueError:
            sig_name = str(sig)
        return {"status": "failed", "error": f"Worker killed by signal {sig_name}"}

    code = proc.returncode if proc.returncode is not None else -1
    return {"status": "failed", "error": f"Worker exited without result (code={code})"}


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
