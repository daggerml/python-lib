from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from daggerml._internal.types import DmlRepoError
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib.executor_state import LocalState
from daggerml.contrib.supervisor import _launch, run


@pytest.fixture(autouse=True)
def _reset_registry():
    ereg._reset_for_tests()
    yield
    ereg._reset_for_tests()


def _cmd_payload(cmd: Any) -> dict[str, Any]:
    return {
        "version": 1,
        "cache_key": f"cache:key:{uuid4()}",
        "cmd": cmd,
        "remote": {"root": "s3://bucket/root", "cache": "cache-ns"},
        "comms": {"kind": "local", "spec": {"cache_dir": "/tmp/dml-comms"}},
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


def test_supervisor_run_updates_state_heartbeat_and_terminal(tmp_path):
    cache_key = f"cache:key:{uuid4()}"
    payload = {
        "version": 1,
        "cache_key": cache_key,
        "cmd": [
            sys.executable,
            "-c",
            'import pathlib; pathlib.Path(\'result.json\').write_text(\'{"status":"succeeded","error":null}\')',
        ],
        "remote": {"root": "s3://bucket/root", "cache": "cache-ns"},
        "comms": {"kind": "local", "spec": {"cache_dir": str(tmp_path)}},
    }
    result = run(payload, heartbeat_s=0.01)
    assert result == {"status": "succeeded", "error": None}
    record = LocalState(cache_key, cache_dir=str(tmp_path)).get()
    assert isinstance(record, dict)
    assert record["status"] == "succeeded"
