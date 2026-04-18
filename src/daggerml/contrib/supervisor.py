from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Mapping

from daggerml._internal import DmlOps
from daggerml._internal.types import DmlRepoError
from daggerml.contrib.executor_state import ExecutionState


def _parse_cmd_payload(
    payload: dict[str, Any],
) -> tuple[str, list[str], dict[str, str], dict[str, str]]:
    allowed = {"version", "cache_key", "cmd", "remote", "env"}
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise DmlRepoError(f"Supervisor payload has unknown fields: {', '.join(unknown)}")

    version = payload.get("version")
    if version != 2:
        raise DmlRepoError("Supervisor payload version must be 2")

    cache_key = payload.get("cache_key")
    if not isinstance(cache_key, str) or not cache_key:
        raise DmlRepoError("Supervisor payload cache_key must be a non-empty string")

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
    return cache_key, cmd, merged_env, {"root": remote["root"]}


def _launch(payload: dict[str, Any]) -> dict[str, Any]:
    cache_key, cmd, env, remote = _parse_cmd_payload(payload)
    workdir = tempfile.mkdtemp(prefix=f"dml-supervisor-{cache_key[:8]}-")
    repo_dir = Path(workdir) / "repo"
    with DmlOps.create(str(repo_dir), user="worker"):
        pass
    env = dict(env)
    env["DML_REMOTE_ROOT"] = remote["root"]
    env["DML_REPO"] = str(repo_dir)
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


def _record_terminal_result(record: Mapping[str, Any]) -> dict[str, Any] | None:
    status = record.get("status")
    if status == "succeeded":
        dag_id = record.get("dag_id")
        return {"status": "succeeded", "error": None, "dag_id": dag_id}
    if status == "failed":
        return {"status": "failed", "error": record.get("error")}
    if status == "done":
        dag_id = record.get("dag_id")
        if dag_id is not None:
            return {"status": "succeeded", "error": None, "dag_id": dag_id}
        return {"status": "failed", "error": record.get("error")}
    return None


def _persist_terminal(cache_key: str, terminal: dict[str, Any], *, retry_s: float) -> dict[str, Any]:
    while True:
        es = ExecutionState(cache_key)
        record = es.get()
        if record is not None:
            observed = _record_terminal_result(record)
            if observed is not None:
                return observed
        if not es.lock():
            time.sleep(retry_s)
            continue
        try:
            record = es.get()
            if record is not None:
                observed = _record_terminal_result(record)
                if observed is not None:
                    return observed
            if terminal["status"] == "succeeded":
                if es.mark_succeeded(terminal["dag_id"]):
                    return terminal
            elif terminal["status"] == "failed":
                if es.mark_failed(terminal["error"]):
                    return terminal
        finally:
            es.unlock()
        time.sleep(retry_s)


def run(payload: dict[str, Any], *, heartbeat_s: float = 0.25) -> dict[str, Any]:
    launched = _launch(payload)
    cache_key = payload["cache_key"]
    pid = launched["pid"]
    result_path = Path(launched["result_path"])

    while True:
        try:
            done_pid, status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            done_pid, status = pid, 0

        if done_pid == 0:
            # Worker still running — refresh heartbeat
            es = ExecutionState(cache_key)
            if es.lock():
                try:
                    es.heartbeat()
                finally:
                    es.unlock()
            time.sleep(heartbeat_s)
            continue

        # Worker exited
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

        return _persist_terminal(cache_key, terminal, retry_s=heartbeat_s)


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
