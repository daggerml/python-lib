from __future__ import annotations

import json
import sys
from typing import Any
from uuid import uuid4

import pytest

from daggerml._internal.types import DmlRepoError
from daggerml.contrib.supervisor import (
    _parse_cmd_payload,
    _validate_output,
    run,
)

pytestmark = pytest.mark.slow

REAL_DAG_ID = "d" * 64


def _cmd_payload(cmd: Any) -> dict[str, Any]:
    return {
        "version": 2,
        "cache_key": f"cache:key:{uuid4()}",
        "execution_id": uuid4().hex,
        "cmd": cmd,
        "remote": {"root": "s3://bucket/root"},
    }


@pytest.mark.parametrize("bad_cmd", [None, [], [1], [""], "python -m mod"])  # type: ignore[list-item]
def test_supervisor_launch_rejects_malformed_cmd(bad_cmd):
    with pytest.raises(DmlRepoError, match=r"cmd must be a non-empty list\[str\]"):
        _parse_cmd_payload(_cmd_payload(bad_cmd))


def test_supervisor_payload_rejects_version_1():
    with pytest.raises(DmlRepoError, match=r"version must be 2"):
        _parse_cmd_payload({**_cmd_payload([sys.executable, "-c", "pass"]), "version": 1})


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
        "version": 2,
        "cache_key": f"cache:key:{uuid4()}",
        "execution_id": uuid4().hex,
        "cmd": [sys.executable, "-c", script],
        "remote": {"root": "s3://bucket/root"},
    }
    result = run(payload)
    assert result == {"status": "succeeded", "error": None, "dag_id": REAL_DAG_ID}


def test_supervisor_run_returns_failed_when_worker_exits_without_result():
    payload = {
        "version": 2,
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
        "version": 2,
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
        "version": 2,
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
        "version": 2,
        "cache_key": f"cache:key:{uuid4()}",
        "execution_id": uuid4().hex,
        "cmd": [sys.executable, "-c", script],
        "remote": {"root": "s3://bucket/root"},
    }
    result = run(payload)
    assert result == {"status": "failed", "error": "worker error"}
