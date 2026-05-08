from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any
from uuid import uuid4

import boto3
import pytest

from daggerml._internal.types import DmlRepoError
from daggerml.contrib.supervisor import (
    _CLOUDWATCH_EVENT_OVERHEAD_BYTES,
    _CLOUDWATCH_MAX_BATCH_BYTES,
    _CLOUDWATCH_MAX_MESSAGE_BYTES,
    _CloudWatchStream,
    _parse_cmd_payload,
    _validate_output,
    run,
)

pytestmark = pytest.mark.slow

REAL_DAG_ID = "d" * 64


def _cmd_payload(cmd: Any) -> dict[str, Any]:
    return {
        "version": 0,
        "cache_key": f"cache:key:{uuid4()}",
        "execution_id": uuid4().hex,
        "cmd": cmd,
        "remote": {"root": "s3://bucket/root"},
    }


def _log_messages(*, cache_key: str, stream_kind: str) -> list[str]:
    client = boto3.client("logs", endpoint_url=os.environ["AWS_ENDPOINT_URL"])
    response = client.get_log_events(logGroupName="dml", logStreamName=f"/run/{cache_key}/{stream_kind}")
    return [event["message"] for event in response["events"]]


@pytest.mark.parametrize("bad_cmd", [None, [], [1], [""], "python -m mod"])  # type: ignore[list-item]
def test_supervisor_launch_rejects_malformed_cmd(bad_cmd):
    with pytest.raises(DmlRepoError, match=r"cmd must be a non-empty list\[str\]"):
        _parse_cmd_payload(_cmd_payload(bad_cmd))


def test_supervisor_payload_rejects_unknown_top_level_fields():
    payload = _cmd_payload([sys.executable, "-c", "pass"])
    payload["extra"] = "nope"
    with pytest.raises(DmlRepoError, match=r"unknown fields: extra"):
        _parse_cmd_payload(payload)


def test_supervisor_payload_rejects_unknown_remote_fields():
    payload = _cmd_payload([sys.executable, "-c", "pass"])
    payload["remote"] = {"root": "s3://bucket/root", "cache": "cache-ns"}
    with pytest.raises(DmlRepoError, match=r"remote has unknown fields: cache"):
        _parse_cmd_payload(payload)


def test_supervisor_validate_output_requires_dag_id_on_success():
    with pytest.raises(DmlRepoError, match=r"dag_id"):
        _validate_output({"status": "succeeded", "error": None})


def test_supervisor_validate_output_rejects_running_only_status_after_worker_exit():
    with pytest.raises(DmlRepoError, match=r"succeeded\|failed"):
        _validate_output({"status": "running", "error": None, "state": {}})


def test_supervisor_validate_output_rejects_canceled_status():
    with pytest.raises(DmlRepoError, match=r"succeeded\|failed"):
        _validate_output({"status": "canceled", "error": None})


def test_supervisor_run_succeeds_when_worker_writes_result():
    result_json = json.dumps({"status": "succeeded", "error": None, "dag_id": REAL_DAG_ID})
    script = (
        f"import pathlib; pathlib.Path('result.json').write_text({result_json!r})"
    )
    payload = {
        "version": 0,
        "cache_key": f"cache:key:{uuid4()}",
        "execution_id": uuid4().hex,
        "cmd": [sys.executable, "-c", script],
        "remote": {"root": "s3://bucket/root"},
    }
    result = run(payload)
    assert result == {"status": "succeeded", "error": None, "dag_id": REAL_DAG_ID}


def test_supervisor_run_returns_failed_when_worker_exits_without_result():
    payload = {
        "version": 0,
        "cache_key": f"cache:key:{uuid4()}",
        "execution_id": uuid4().hex,
        "cmd": [sys.executable, "-c", "pass"],
        "remote": {"root": "s3://bucket/root"},
    }
    result = run(payload)
    assert result["status"] == "failed"
    assert "result" in result["error"].lower() or "code" in result["error"].lower()


def test_supervisor_run_returns_failed_when_worker_writes_running_status():
    script = "import pathlib; pathlib.Path('result.json').write_text('{\"status\":\"running\",\"error\":null}')"
    payload = {
        "version": 0,
        "cache_key": f"cache:key:{uuid4()}",
        "execution_id": uuid4().hex,
        "cmd": [sys.executable, "-c", script],
        "remote": {"root": "s3://bucket/root"},
    }
    result = run(payload)
    assert result["status"] == "failed"
    assert "succeeded|failed" in result["error"]


def test_supervisor_run_returns_failed_when_worker_crashes():
    payload = {
        "version": 0,
        "cache_key": f"cache:key:{uuid4()}",
        "execution_id": uuid4().hex,
        "cmd": [sys.executable, "-c", "raise RuntimeError('boom')"],
        "remote": {"root": "s3://bucket/root"},
    }
    result = run(payload)
    assert result["status"] == "failed"


def test_supervisor_run_returns_failed_result_from_worker():
    script = (
        "import pathlib; "
        "pathlib.Path('result.json').write_text("
        "'{\"status\":\"failed\",\"error\":\"worker error\"}'"
        ")"
    )
    payload = {
        "version": 0,
        "cache_key": f"cache:key:{uuid4()}",
        "execution_id": uuid4().hex,
        "cmd": [sys.executable, "-c", script],
        "remote": {"root": "s3://bucket/root"},
    }
    result = run(payload)
    assert result == {"status": "failed", "error": "worker error"}


def test_supervisor_streams_stdout_and_stderr_to_cloudwatch_and_local_files(monkeypatch):
    seen_paths: dict[str, str] = {}
    real_mkdtemp = tempfile.mkdtemp

    def capture_mkdtemp(*args, **kwargs):
        path = real_mkdtemp(*args, **kwargs)
        seen_paths["workdir"] = path
        return path

    monkeypatch.setattr("daggerml.contrib.supervisor.tempfile.mkdtemp", capture_mkdtemp)
    cache_key = f"cache:key:{uuid4()}"
    script = "\n".join(
        [
            "import pathlib, sys, time",
            "sys.stdout.write('stdout-line\\n')",
            "sys.stdout.flush()",
            "time.sleep(0.05)",
            "sys.stderr.write('stderr-line\\n')",
            "sys.stderr.flush()",
            "pathlib.Path('result.json').write_text("
            f"{json.dumps({'status': 'succeeded', 'error': None, 'dag_id': REAL_DAG_ID})!r}"
            ")",
            "",
        ]
    )
    payload = {
        "version": 0,
        "cache_key": cache_key,
        "execution_id": uuid4().hex,
        "cmd": [sys.executable, "-c", script],
        "remote": {"root": "s3://bucket/root"},
    }

    result = run(payload)

    assert result == {"status": "succeeded", "error": None, "dag_id": REAL_DAG_ID}
    workdir = Path(seen_paths["workdir"])
    assert workdir.joinpath("stdout.log").read_text() == "stdout-line\n"
    assert workdir.joinpath("stderr.log").read_text() == "stderr-line\n"

    stdout_messages = _log_messages(cache_key=cache_key, stream_kind="stdout")
    stderr_messages = _log_messages(cache_key=cache_key, stream_kind="stderr")
    assert json.loads(stdout_messages[0]) == {
        "cache_key": cache_key,
        "event": "stream_start",
        "execution_id": payload["execution_id"],
        "stream": "stdout",
    }
    assert stdout_messages[1] == "stdout-line\n"
    assert json.loads(stdout_messages[-1]) == {
        "cache_key": cache_key,
        "event": "stream_end",
        "execution_id": payload["execution_id"],
        "stream": "stdout",
        "terminal_status": "succeeded",
    }
    assert stderr_messages[1] == "stderr-line\n"
    assert json.loads(stderr_messages[-1]) == {
        "cache_key": cache_key,
        "event": "stream_end",
        "execution_id": payload["execution_id"],
        "stream": "stderr",
        "terminal_status": "succeeded",
    }


def test_supervisor_cloudwatch_delivery_failure_is_non_fatal(monkeypatch, caplog):
    class FailingLogsClient:
        def create_log_group(self, **kwargs):
            return None

        def create_log_stream(self, **kwargs):
            return None

        def put_log_events(self, **kwargs):
            raise RuntimeError("cw down")

    monkeypatch.setattr("daggerml.contrib.supervisor._create_logs_client", lambda: FailingLogsClient())
    result_json = json.dumps({"status": "succeeded", "error": None, "dag_id": REAL_DAG_ID})
    script = (
        "import pathlib,sys; sys.stdout.write('hello\\n'); sys.stdout.flush(); "
        f"pathlib.Path('result.json').write_text({result_json!r})"
    )
    payload = {
        "version": 0,
        "cache_key": f"cache:key:{uuid4()}",
        "execution_id": uuid4().hex,
        "cmd": [sys.executable, "-c", script],
        "remote": {"root": "s3://bucket/root"},
    }

    with caplog.at_level("WARNING"):
        result = run(payload)

    assert result == {"status": "succeeded", "error": None, "dag_id": REAL_DAG_ID}
    assert any("CloudWatch logging disabled" in message for message in caplog.messages)


def test_supervisor_cloudwatch_initialization_failure_is_non_fatal(monkeypatch, caplog):
    monkeypatch.setattr(
        "daggerml.contrib.supervisor._create_logs_client",
        lambda: (_ for _ in ()).throw(RuntimeError("no cw")),
    )
    result_json = json.dumps({"status": "succeeded", "error": None, "dag_id": REAL_DAG_ID})
    script = (
        "import pathlib,sys; sys.stderr.write('hello\\n'); sys.stderr.flush(); "
        f"pathlib.Path('result.json').write_text({result_json!r})"
    )
    payload = {
        "version": 0,
        "cache_key": f"cache:key:{uuid4()}",
        "execution_id": uuid4().hex,
        "cmd": [sys.executable, "-c", script],
        "remote": {"root": "s3://bucket/root"},
    }

    with caplog.at_level("WARNING"):
        result = run(payload)

    assert result == {"status": "succeeded", "error": None, "dag_id": REAL_DAG_ID}
    assert any("CloudWatch logging disabled" in message for message in caplog.messages)


def test_cloudwatch_stream_batches_events_before_delivery():
    calls: list[list[str]] = []

    class RecordingLogsClient:
        def create_log_group(self, **kwargs):
            return None

        def create_log_stream(self, **kwargs):
            return None

        def put_log_events(self, **kwargs):
            calls.append([event["message"] for event in kwargs["logEvents"]])
            return {}

    stream = _CloudWatchStream(cache_key="ck-batch", execution_id="exec-batch", stream_kind="stdout")
    stream._client = RecordingLogsClient()
    stream._pending_events.clear()
    stream._pending_bytes = 0

    second_event_bytes = len("second".encode("utf-8")) + _CLOUDWATCH_EVENT_OVERHEAD_BYTES
    large_message = "x" * (_CLOUDWATCH_MAX_BATCH_BYTES - _CLOUDWATCH_EVENT_OVERHEAD_BYTES - second_event_bytes + 1)
    stream.emit(large_message)
    stream.emit("second")
    stream.close(terminal_status="succeeded")

    assert len(calls) == 2
    assert calls[0] == [large_message]
    assert calls[1][0] == "second"
    assert json.loads(calls[1][1])["event"] == "stream_end"


def test_cloudwatch_stream_splits_single_event_that_exceeds_size_limit():
    class RecordingLogsClient:
        def __init__(self):
            self.calls: list[list[str]] = []

        def create_log_group(self, **kwargs):
            return None

        def create_log_stream(self, **kwargs):
            return None

        def put_log_events(self, **kwargs):
            self.calls.append([event["message"] for event in kwargs["logEvents"]])
            return {}

    client = RecordingLogsClient()
    stream = _CloudWatchStream(cache_key="ck-oversized", execution_id="exec-oversized", stream_kind="stdout")
    stream._client = client
    stream._pending_events.clear()
    stream._pending_bytes = 0
    too_large_message = "x" * (_CLOUDWATCH_MAX_MESSAGE_BYTES + 1)

    stream.emit(too_large_message)
    stream.emit("tail")
    stream.close(terminal_status="succeeded")

    all_messages = [message for call in client.calls for message in call]
    oversized_chunks = [message for message in all_messages if set(message) == {"x"}]
    assert len(oversized_chunks) == 2
    assert "".join(oversized_chunks) == too_large_message
    assert "tail" in all_messages
