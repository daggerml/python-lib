from __future__ import annotations

import sys
import threading
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from daggerml._internal.types import DmlRepoError
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib.executor_state import ExecutionState
from daggerml.contrib.supervisor import (
    _launch,
    _parse_cmd_payload,
    _persist_terminal,
    _validate_output,
    run,
)

REAL_DAG_ID = "d" * 64


@pytest.fixture(autouse=True)
def _reset_registry():
    ereg._reset_for_tests()
    yield
    ereg._reset_for_tests()


def _cmd_payload(cmd: Any) -> dict[str, Any]:
    return {
        "version": 2,
        "cache_key": f"cache:key:{uuid4()}",
        "cmd": cmd,
        "remote": {"root": "s3://bucket/root"},
    }


@pytest.mark.parametrize("bad_cmd", [None, [], [1], [""], "python -m mod"])  # type: ignore[list-item]
def test_supervisor_launch_rejects_malformed_cmd(bad_cmd):
    with pytest.raises(DmlRepoError, match=r"cmd must be a non-empty list\[str\]"):
        _launch(_cmd_payload(bad_cmd))


def test_supervisor_launch_sets_up_workspace_and_logs():
    payload = _cmd_payload([sys.executable, "-c", "print('ok')"])
    result = _launch(payload)
    assert result["status"] == "running"
    assert result["error"] is None
    assert isinstance(result["pid"], int)
    assert Path(result["workdir"]).exists()
    assert Path(result["repo_dir"]).exists()
    assert Path(result["stdout_path"]).exists()
    assert Path(result["stderr_path"]).exists()


def test_supervisor_run_updates_state_heartbeat_and_terminal():
    cache_key = f"cache:key:{uuid4()}"
    # Seed state for supervisor to use
    ExecutionState.upsert(cache_key, "argv://ptr")
    # Mark running so supervisor can write heartbeat and terminal
    es = ExecutionState(cache_key)
    assert es.lock()
    es.mark_running()
    es.unlock()

    payload = {
        "version": 2,
        "cache_key": cache_key,
        "cmd": [
            sys.executable,
            "-c",
            (
                "import pathlib; pathlib.Path('result.json').write_text"
                f'(\'{{"status":"succeeded","error":null,"dag_id":"{REAL_DAG_ID}"}}\')'
            ),
        ],
        "remote": {"root": "s3://bucket/root"},
    }
    result = run(payload, heartbeat_s=0.01)
    assert result == {"status": "succeeded", "error": None, "dag_id": REAL_DAG_ID}
    record = ExecutionState(cache_key).get()
    assert record is not None
    assert record["status"] == "succeeded"
    assert record["dag_id"] == REAL_DAG_ID


def test_supervisor_run_retries_until_terminal_write_succeeds():
    cache_key = f"cache:key:{uuid4()}"
    ExecutionState.upsert(cache_key, "argv://ptr")
    holder = ExecutionState(cache_key)
    assert holder.lock()
    assert holder.mark_running()
    holder.unlock()

    blocker = ExecutionState(cache_key)
    assert blocker.lock()

    def release_lock():
        time.sleep(0.05)
        blocker.unlock()

    thread = threading.Thread(target=release_lock)
    thread.start()
    try:
        payload = {
            "version": 2,
            "cache_key": cache_key,
            "cmd": [
                sys.executable,
                "-c",
                (
                    "import pathlib; pathlib.Path('result.json').write_text"
                    f'(\'{{"status":"succeeded","error":null,"dag_id":"{REAL_DAG_ID}"}}\')'
                ),
            ],
            "remote": {"root": "s3://bucket/root"},
        }
        result = run(payload, heartbeat_s=0.01)
    finally:
        thread.join()

    assert result == {"status": "succeeded", "error": None, "dag_id": REAL_DAG_ID}
    record = ExecutionState(cache_key).get()
    assert record is not None
    assert record["status"] == "succeeded"


def test_persist_terminal_returns_already_terminal_record():
    cache_key = f"cache:key:{uuid4()}"
    ExecutionState.upsert(cache_key, "argv://ptr")
    es = ExecutionState(cache_key)
    assert es.lock()
    assert es.mark_running()
    assert es.mark_succeeded(REAL_DAG_ID)
    es.unlock()

    result = _persist_terminal(
        cache_key,
        {"status": "failed", "error": "should not overwrite"},
        retry_s=0.01,
    )

    assert result == {"status": "succeeded", "error": None, "dag_id": REAL_DAG_ID}


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


def test_supervisor_validate_output_rejects_pending_after_worker_exit():
    with pytest.raises(DmlRepoError, match=r"succeeded\|failed"):
        _validate_output({"status": "pending", "error": None})


def test_supervisor_run_marks_stranded_running_result_failed():
    cache_key = f"cache:key:{uuid4()}"
    ExecutionState.upsert(cache_key, "argv://ptr")
    es = ExecutionState(cache_key)
    assert es.lock()
    es.mark_running()
    es.unlock()

    payload = {
        "version": 2,
        "cache_key": cache_key,
        "cmd": [
            sys.executable,
            "-c",
            'import pathlib; pathlib.Path(\'result.json\').write_text(\'{"status":"running","error":null}\')',
        ],
        "remote": {"root": "s3://bucket/root"},
    }

    result = run(payload, heartbeat_s=0.01)
    assert result["status"] == "failed"
    assert "succeeded|failed" in result["error"]
    record = ExecutionState(cache_key).get()
    assert record is not None
    assert record["status"] == "failed"


def test_supervisor_validate_output_rejects_canceled_status():
    with pytest.raises(DmlRepoError, match=r"succeeded\|failed"):
        _validate_output({"status": "canceled", "error": None})
