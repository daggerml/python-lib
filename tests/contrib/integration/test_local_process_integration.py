"""Real local adapter, supervisor, script worker and scratch lifecycle."""

import json
import os
import signal
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import pytest

import daggerml.api as api
from daggerml import Runnable, Uri
from daggerml.contrib.api import funkify
from daggerml.contrib.codecs import DelayedActionCodec
from daggerml.contrib.executors._ownership import _current_owner
from daggerml.contrib.executors._services import select_service, service_active, start_service, stop_service
from daggerml.contrib.s3 import S3Store


@funkify(uri="script")
def worker_pid(dag, value):
    import os

    return {"pid": os.getpid(), "value": value.value()}


@funkify(uri="script")
def worker_artifact(dag):
    import daggerml as dml
    from daggerml.contrib.s3 import S3Store

    uri = S3Store().put(data=b"job artifact")
    return {"uri": uri.uri, "root": dml.get_default_dml().config.show()["remote"]["root"]}


@funkify(uri="script", prepop={"child": worker_pid})
def nested_worker(dag, value):
    return dag.child(value)


@pytest.mark.parametrize("manager_free", [False, True], ids=["auto-backend", "pgroup"])
def test_nested_script_calls_return_without_waiting_for_lock_expiry(runtime_world, monkeypatch, manager_free):
    monkeypatch.setattr(
        api, "_codecs",
        [(1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()), (0, 3, api.MiscPyTypeCodec())],
    )
    if manager_free:
        # Keep installed adapter commands, but exclude OS service-manager tools.
        monkeypatch.setenv("PATH", str(Path(sys.executable).parent))
        assert select_service("probe", runtime_world.root) is None
    dml = runtime_world.publisher
    with api.new("nested-call", dml=dml) as dag:
        result = dag.put(nested_worker)(42, timeout=30_000, sleep=lambda: 0)
        dag.commit(result)
    assert api.load("nested-call", dml=dml).result.value()["value"] == 42
    key = dml.dag.describe(result.context().ref)["cache_key"]
    execution = dml.cache.describe(key)["execution"]
    state = dml.runtime.read_execution_record(execution)["driver"]["adapter_state"]
    expected = select_service(execution.id(), runtime_world.root)
    assert state["handle"]["backend"] == (expected["backend"] if expected else "pgroup")


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
    record = dml.runtime.read_execution_record(execution)
    assert record["state"]["lifecycle"] == "succeeded"
    state = record["driver"]["adapter_state"]
    assert state["owner"] == _current_owner()
    if state and isinstance(state, dict) and state.get("workdir"):
        assert not Path(state["workdir"]).exists()


def test_local_adapter_wire_cleans_active_supervisor_across_fresh_processes(tmp_path):
    workdir = tmp_path / "scratch"
    workdir.mkdir()
    (workdir / "worker.log").write_text("scratch")
    worker = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
    state = {"owner": _current_owner(), "handle": {"backend": "pgroup", "pgid": worker.pid}, "workdir": str(workdir)}
    request = {
        "operation": "cleanup", "cache_key": "process-cleanup", "execution_id": "process-cleanup",
        "runnable": asdict(Runnable(target=Uri("script"), kwargs={}, adapter="dml-local-adapter")),
        "remote": {"root": "s3://test-bucket/test-prefix"},
        "scratch_uri": "s3://test-bucket/test-prefix/scratch",
        "adapter_state": state, "result_ref": "dag:result",
    }

    def send():
        proc = subprocess.run(
            [str(Path(sys.executable).with_name("dml-local-adapter")), "-i", "-", "-o", "-"],
            input=json.dumps(request), text=True, capture_output=True, timeout=10,
        )
        assert proc.returncode == 0, proc.stderr
        return json.loads(proc.stdout)

    try:
        first = send()
        assert first["status"] == "success"
        assert first["adapter_state"] == state
        worker.wait(timeout=10)
        assert not workdir.exists()
    finally:
        if worker.poll() is None:
            os.killpg(worker.pid, signal.SIGTERM)
            worker.wait(timeout=10)
    second = send()
    assert second["status"] == "success"
    assert not workdir.exists()


def test_script_cancel_reaps_worker_process_group_via_adapter_wire(tmp_path):
    parent = subprocess.Popen(
        [sys.executable, "-u", "-c", "import subprocess,sys,time; "
         "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']); "
         "print(child.pid,flush=True); time.sleep(30)"],
        stdout=subprocess.PIPE, start_new_session=True, text=True,
    )
    assert parent.stdout is not None
    child_pid = int(parent.stdout.readline().strip())
    workdir = tmp_path / "cancel-scratch"
    workdir.mkdir()
    request = {
        "operation": "cancel", "cache_key": "process-cancel", "execution_id": "process-cancel",
        "runnable": asdict(Runnable(target=Uri("script"), kwargs={}, adapter="dml-local-adapter")),
        "remote": {"root": "s3://test-bucket/test-prefix"},
        "scratch_uri": "s3://test-bucket/test-prefix/scratch",
        "adapter_state": {"owner": _current_owner(), "handle": {"backend": "pgroup", "pgid": parent.pid},
                          "workdir": str(workdir)},
        "argv_ref": "node-argv:process-cancel", "requested_by": "tester",
    }
    try:
        proc = subprocess.run(
            [str(Path(sys.executable).with_name("dml-local-adapter")), "-i", "-", "-o", "-"],
            input=json.dumps(request), text=True, capture_output=True, timeout=10,
        )
        assert proc.returncode == 0, proc.stderr
        assert json.loads(proc.stdout)["status"] == "cancelled"
        parent.wait(timeout=5)
        assert not workdir.exists()
        try:
            os.kill(child_pid, 0)
        except ProcessLookupError:
            pass
        else:
            pytest.xfail("script cancellation acknowledges before its child process is reaped")
    finally:
        try:
            os.killpg(parent.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            os.kill(child_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        parent.wait(timeout=5)


@pytest.mark.parametrize("operation", ["cleanup", "cancel"])
def test_service_teardown_uses_saved_handle_across_adapter_processes(tmp_path, operation):
    execution_id = uuid4().hex
    remote = {"root": "s3://test-bucket/test-prefix"}
    handle = select_service(execution_id, remote["root"])
    if handle is None:
        pytest.skip("No service-manager tools installed")
    workdir = tmp_path / "service-scratch"
    workdir.mkdir()
    ready = workdir / "ready"
    command = [sys.executable, "-c",
               f"from pathlib import Path; import time; Path({str(ready)!r}).touch(); time.sleep(60)"]
    state = {"owner": _current_owner(), "handle": handle, "workdir": str(workdir)}
    request = {
        "operation": operation, "cache_key": "service-teardown", "execution_id": execution_id,
        "runnable": asdict(Runnable(target=Uri("script"), kwargs={}, adapter="dml-local-adapter")),
        "remote": remote, "scratch_uri": remote["root"] + "/scratch", "adapter_state": state,
    }
    if operation == "cleanup":
        request["result_ref"] = "dag:result"
    else:
        request.update(argv_ref="node-argv:service-teardown", requested_by="tester")

    def send():
        proc = subprocess.run(
            [str(Path(sys.executable).with_name("dml-local-adapter")), "-i", "-", "-o", "-"],
            input=json.dumps(request), text=True, capture_output=True, timeout=60,
        )
        assert proc.returncode == 0, proc.stderr
        result = json.loads(proc.stdout)
        assert result["adapter_state"] == state
        return result

    try:
        start_service(handle, command, workdir, dict(os.environ))
        deadline = monotonic() + 10
        while not ready.exists():
            assert monotonic() < deadline, "service did not start"
            sleep(0.1)
        assert service_active(handle)
        expected = "success" if operation == "cleanup" else "cancelled"
        deadline = monotonic() + 30
        while (result := send())["status"] == "retry":
            assert monotonic() < deadline, "service teardown did not complete"
            sleep(0.1)
        assert result["status"] == expected, result
        assert not service_active(handle)
        assert not workdir.exists()
        assert send()["status"] == expected
    finally:
        stop_service(handle)
