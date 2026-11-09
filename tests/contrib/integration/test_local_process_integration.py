"""Real local adapter, OS-managed supervisor, script worker and scratch lifecycle."""

import json
import os
import subprocess
import sys
import time
import uuid
from dataclasses import asdict
from pathlib import Path

import pytest

import daggerml.api as api
from daggerml import Runnable, Uri
from daggerml.contrib.api import funkify
from daggerml.contrib.codecs import DelayedActionCodec
from daggerml.contrib.executors._ownership import _current_owner
from daggerml.contrib.executors._services import service_active, service_handle, start_service, stop_service
from daggerml.contrib.s3 import S3Store


@funkify(uri="script")
def worker_pid(dag, value):
    import os

    return {"pid": os.getpid(), "value": value.value(), "execution_id": os.environ["DML_EXECUTION_ID"]}


@funkify(uri="script")
def worker_artifact(dag):
    import daggerml as dml
    from daggerml.contrib.s3 import S3Store

    uri = S3Store().put(data=b"job artifact")
    return {"uri": uri.uri, "root": dml.get_default_dml().config.show()["remote"]["root"]}


def test_script_artifacts_use_job_root_despite_conflicting_environment(runtime_world, monkeypatch):
    dml = runtime_world.publisher
    monkeypatch.setattr(
        api, "_codecs",
        [(1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()), (0, 3, api.MiscPyTypeCodec())],
    )
    with api.use_default_dml(dml), api.new("job-artifact", dml=dml) as dag:
        runnable = dag.put(worker_artifact)
        monkeypatch.setenv("DML_REMOTE_ROOT", "s3://unrelated-missing-bucket/project")
        result = runnable(sleep=lambda: 0, timeout=30_000)
        dag.commit(result)
    value = api.load("job-artifact", dml=dml).result.value()
    assert value["root"] == runtime_world.root
    assert value["uri"].startswith(runtime_world.root + "/data/")
    assert S3Store.from_remote_root(runtime_world.root).get(value["uri"]) == b"job artifact"


def test_local_adapter_crosses_process_boundary_and_cleans_scratch(runtime_world, monkeypatch):
    monkeypatch.setattr(
        api, "_codecs",
        [(1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()), (0, 3, api.MiscPyTypeCodec())],
    )
    dml = runtime_world.publisher
    with api.new("process-boundary", dml=dml) as dag:
        result = dag.put(worker_pid)(42, sleep=lambda: 0, timeout=30_000)
        dag.commit(result)
    value = api.load("process-boundary", dml=dml).result.value()
    assert value["pid"] != os.getpid()
    assert value["value"] == 42
    key = dml.dag.describe(result.context().ref)["cache_key"]
    execution = dml.cache.describe(key)["execution"]
    assert value["execution_id"] == execution.id()
    record = dml.runtime.read_execution_record(execution)
    assert record["state"]["lifecycle"] == "succeeded"
    state = record["driver"]["adapter_state"]
    assert state["owner"] == _current_owner()
    assert state["service"]["name"]
    assert "pid" not in state
    assert not Path(state["workdir"]).exists()


@pytest.mark.parametrize("operation", ["cleanup", "cancel"])
def test_local_adapter_wire_stops_named_service_across_fresh_processes(tmp_path, operation):
    handle = service_handle(uuid.uuid4().hex, "s3://test-bucket/project")
    workdir = tmp_path / "scratch"
    workdir.mkdir()
    start_service(handle, [sys.executable, "-c", "import time; time.sleep(60)"], workdir, dict(os.environ))
    state = {"owner": _current_owner(), "service": handle, "workdir": str(workdir)}
    request = {
        "operation": operation, "cache_key": "ck", "execution_id": "exec",
        "runnable": asdict(Runnable(target=Uri("script"), kwargs={}, adapter="dml-local-adapter")),
        "remote": {"root": "s3://test-bucket/project"}, "scratch_uri": "s3://test-bucket/scratch",
        "adapter_state": state,
    }
    request.update({"result_ref": "dag:result"} if operation == "cleanup" else
                   {"argv_ref": "node-argv:argv", "requested_by": "tester"})
    try:
        for _ in range(2):
            proc = subprocess.run(
                [str(Path(sys.executable).with_name("dml-local-adapter")), "-i", "-", "-o", "-"],
                input=json.dumps(request), text=True, capture_output=True, timeout=40,
            )
            assert proc.returncode == 0, proc.stderr
            result = json.loads(proc.stdout)
            assert result["status"] == ("success" if operation == "cleanup" else "cancelled"), result
            assert result["adapter_state"] == state
        assert not workdir.exists()
        assert not service_active(handle)
    finally:
        stop_service(handle)


def test_service_manager_stops_workers_when_supervisor_exits(tmp_path):
    handle = service_handle(uuid.uuid4().hex, "s3://test-bucket/project")
    marker = tmp_path / "heartbeat"
    child = ("import pathlib,time,sys\np=pathlib.Path(sys.argv[1])\n"
             "while True:\n p.write_text(str(time.time_ns()))\n time.sleep(.05)")
    parent = (
        "import subprocess,sys,time,pathlib\n"
        "subprocess.Popen([sys.executable,'-c',sys.argv[1],sys.argv[2]])\n"
        "while not pathlib.Path(sys.argv[2]).exists(): time.sleep(.01)\n"
    )
    try:
        start_service(handle, [sys.executable, "-c", parent, child, str(marker)], tmp_path, dict(os.environ))
        deadline = time.monotonic() + 15
        while service_active(handle):
            assert time.monotonic() < deadline
            time.sleep(.05)
        assert marker.exists()
        before = marker.read_text()
        time.sleep(.2)
        assert marker.read_text() == before
    finally:
        stop_service(handle)
