from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from daggerml._internal import Dml, DmlRepoError

logger = logging.getLogger(__name__)

_CLOUDWATCH_LOG_GROUP = "dml"
_CLOUDWATCH_MAX_BATCH_BYTES = 1_048_576
_CLOUDWATCH_MAX_MESSAGE_BYTES = 1_048_576
_CLOUDWATCH_EVENT_OVERHEAD_BYTES = 26
_CLOUDWATCH_MAX_BATCH_COUNT = 10_000


def _create_logs_client() -> Any:
    import boto3

    endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
    kwargs = {"endpoint_url": endpoint_url} if endpoint_url else {}
    return boto3.client("logs", **kwargs)


def _resource_already_exists(exc: Exception) -> bool:
    return getattr(exc, "response", {}).get("Error", {}).get("Code") == "ResourceAlreadyExistsException"


class _CloudWatchStream:
    def __init__(self, *, cache_key: str, execution_id: str, stream_kind: str):
        self.cache_key = cache_key
        self.execution_id = execution_id
        self.stream_kind = stream_kind
        self.stream_name = f"/run/{cache_key}/{stream_kind}"
        self._client: Any | None = None
        self._enabled = True
        self._sequence_token: str | None = None
        self._pending_events: list[dict[str, Any]] = []
        self._pending_bytes = 0
        self._lock = threading.Lock()
        self._init_client()
        self.emit_lifecycle(event="start")

    @staticmethod
    def _event_bytes(message: str) -> int:
        return len(message.encode("utf-8")) + _CLOUDWATCH_EVENT_OVERHEAD_BYTES

    @staticmethod
    def _split_message(message: str) -> list[str]:
        encoded = message.encode("utf-8")
        if len(encoded) <= _CLOUDWATCH_MAX_MESSAGE_BYTES:
            return [message]

        chunks: list[str] = []
        start = 0
        while start < len(encoded):
            end = min(start + _CLOUDWATCH_MAX_MESSAGE_BYTES, len(encoded))
            while end > start:
                try:
                    chunks.append(encoded[start:end].decode("utf-8"))
                    start = end
                    break
                except UnicodeDecodeError:
                    end -= 1
            else:
                raise AssertionError("failed to split UTF-8 message into valid chunks")
        return chunks

    def _flush_locked(self) -> None:
        if not self._enabled or self._client is None or not self._pending_events:
            return
        params: dict[str, Any] = {
            "logGroupName": _CLOUDWATCH_LOG_GROUP,
            "logStreamName": self.stream_name,
            "logEvents": list(self._pending_events),
        }
        if self._sequence_token is not None:
            params["sequenceToken"] = self._sequence_token
        try:
            response = self._client.put_log_events(**params)
            self._sequence_token = response.get("nextSequenceToken")
            self._pending_events.clear()
            self._pending_bytes = 0
        except Exception as exc:
            self._pending_events.clear()
            self._pending_bytes = 0
            self._disable(f"event delivery failed: {exc}")

    def _init_client(self) -> None:
        try:
            client = _create_logs_client()
            try:
                client.create_log_group(logGroupName=_CLOUDWATCH_LOG_GROUP)
            except Exception as exc:
                if not _resource_already_exists(exc):
                    raise
            try:
                client.create_log_stream(logGroupName=_CLOUDWATCH_LOG_GROUP, logStreamName=self.stream_name)
            except Exception as exc:
                if not _resource_already_exists(exc):
                    raise
            self._client = client
        except Exception as exc:
            self._disable(f"initialization failed: {exc}")

    def _disable(self, reason: str) -> None:
        if not self._enabled:
            return
        self._enabled = False
        self._client = None
        logger.warning("CloudWatch logging disabled for %s: %s", self.stream_name, reason)

    def emit_lifecycle(self, *, event: str, terminal_status: str | None = None) -> None:
        payload = {
            "event": f"stream_{event}",
            "execution_id": self.execution_id,
            "cache_key": self.cache_key,
            "stream": self.stream_kind,
        }
        if terminal_status is not None:
            payload["terminal_status"] = terminal_status
        self.emit(json.dumps(payload, sort_keys=True))

    def emit(self, message: str) -> None:
        if not self._enabled or self._client is None:
            return
        messages = self._split_message(message)
        with self._lock:
            for chunk in messages:
                event_bytes = self._event_bytes(chunk)
                if self._pending_events and (
                    len(self._pending_events) >= _CLOUDWATCH_MAX_BATCH_COUNT
                    or self._pending_bytes + event_bytes > _CLOUDWATCH_MAX_BATCH_BYTES
                ):
                    self._flush_locked()
                    if not self._enabled:
                        return
                event = {"timestamp": round(time.time() * 1000), "message": chunk}
                self._pending_events.append(event)
                self._pending_bytes += event_bytes

    def close(self, *, terminal_status: str) -> None:
        self.emit_lifecycle(event="end", terminal_status=terminal_status)
        with self._lock:
            self._flush_locked()


def _drain_pipe(pipe: Any, *, local_path: Path, sink: _CloudWatchStream) -> None:
    with local_path.open("w") as local_file:
        for line in pipe:
            local_file.write(line)
            local_file.flush()
            sink.emit(line)
    pipe.close()


def _parse_cmd_payload(
    payload: dict[str, Any],
) -> tuple[str, str, list[str], dict[str, str], dict[str, str]]:
    allowed = {"version", "cache_key", "execution_id", "cmd", "remote", "env"}
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise DmlRepoError(f"Supervisor payload has unknown fields: {', '.join(unknown)}")

    version = payload.get("version")
    if version != 0:
        raise DmlRepoError("Supervisor payload version must be 0")

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
    repo_dir.mkdir(parents=True, exist_ok=True)
    Dml.init(str(repo_dir), remote_root=remote["root"], user="worker")
    env = dict(env)
    env["DML_PROJECT_HOME"] = str(repo_dir)
    result_path = Path(workdir) / "result.json"
    stdout_path = Path(workdir) / "stdout.log"
    stderr_path = Path(workdir) / "stderr.log"
    stdout_sink = _CloudWatchStream(cache_key=cache_key, execution_id=execution_id, stream_kind="stdout")
    stderr_sink = _CloudWatchStream(cache_key=cache_key, execution_id=execution_id, stream_kind="stderr")
    proc = subprocess.Popen(
        cmd,
        cwd=workdir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        start_new_session=False,
        close_fds=True,
    )
    assert proc.stdout is not None
    assert proc.stderr is not None
    stdout_thread = threading.Thread(
        target=_drain_pipe,
        kwargs={"pipe": proc.stdout, "local_path": stdout_path, "sink": stdout_sink},
        name=f"dml-supervisor-{execution_id[:8]}-stdout",
    )
    stderr_thread = threading.Thread(
        target=_drain_pipe,
        kwargs={"pipe": proc.stderr, "local_path": stderr_path, "sink": stderr_sink},
        name=f"dml-supervisor-{execution_id[:8]}-stderr",
    )
    stdout_thread.start()
    stderr_thread.start()
    proc.wait()
    stdout_thread.join()
    stderr_thread.join()

    result: dict[str, Any]
    if result_path.exists():
        try:
            parsed = json.loads(result_path.read_text())
            result = _validate_output(parsed)
        except Exception as e:
            result = {"status": "failed", "error": f"Supervisor could not read worker result: {e}"}
    elif proc.returncode is not None and proc.returncode < 0:
        import signal as _signal

        sig = -proc.returncode
        try:
            sig_name = _signal.Signals(sig).name
        except ValueError:
            sig_name = str(sig)
        result = {"status": "failed", "error": f"Worker killed by signal {sig_name}"}
    else:
        code = proc.returncode if proc.returncode is not None else -1
        result = {"status": "failed", "error": f"Worker exited without result (code={code})"}

    terminal_status = str(result.get("status", "failed"))
    stdout_sink.close(terminal_status=terminal_status)
    stderr_sink.close(terminal_status=terminal_status)
    return result


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
