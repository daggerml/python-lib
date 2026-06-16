from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import daggerml.contrib.executors.script as script_mod
from daggerml import Runnable, Uri
from daggerml.api import DmlRepoError
from daggerml.contrib.executors.script import ScriptExecutor


def test_contrib_script_001__script_kwargs_capture_fn_name_and_require_first_dag_parameter():
    def fn(dag, x):
        return x.value()

    kwargs, script = ScriptExecutor._script_kwargs({"fn": fn, "prepop": {}})
    assert kwargs == {"prepop": {}, "fn_name": "fn"}
    assert "def fn" in script

    def bad(x, dag):
        return x

    with pytest.raises(DmlRepoError, match="first 'dag' parameter"):
        ScriptExecutor._script_kwargs({"fn": bad})


def test_contrib_script_002__rendered_source_rejects_pathological_wrapped_functions():
    def fn(dag):
        return 1

    fn.__wrapped__ = fn
    with pytest.raises(ValueError, match="wrapper loop"):
        ScriptExecutor._render_script(fn, extra_objs=[], post_lines=[])


def test_contrib_script_003__resolve_runnable_rejects_sub_and_writes_script_to_s3(monkeypatch):
    seen = {}

    class FakeStore:
        def put(self, *, data, suffix):
            seen.update({"data": data.decode("utf-8"), "suffix": suffix})
            return Uri("s3://bucket/script.py")

    monkeypatch.setattr(script_mod, "S3Store", lambda: FakeStore())

    def fn(dag):
        return 1

    runnable = ScriptExecutor.resolve_runnable("script", {"fn": fn}, None)
    assert runnable.kwargs["script_uri"] == "s3://bucket/script.py"
    assert runnable.kwargs["fn_name"] == "fn"
    assert seen["suffix"] == ".py"

    with pytest.raises(DmlRepoError, match="does not accept sub runnable"):
        ScriptExecutor.resolve_runnable("script", {"fn": fn}, Runnable(target=Uri("inner"), kwargs={}, adapter="x"))


def test_contrib_script_004__start_returns_durable_running_state(monkeypatch):
    class FakePopen:
        pid = 123

        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(script_mod.subprocess, "Popen", FakePopen)
    result = ScriptExecutor().start(
        cache_key="ck",
        execution_id="exec",
        runnable={"target": {"uri": "script"}, "kwargs": {}, "adapter": "dml-local-adapter", "sub": None},
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/scratch",
    )
    assert result["lifecycle"] == "running"
    assert result["dag_id"] is None
    assert result["state"]["pid"] == 123
    assert "workdir" in result["state"]


def test_contrib_script_005__poll_handles_terminal_malformed_and_no_result_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(script_mod.os, "waitpid", lambda pid, flags: (pid, 0))

    success_dir = tmp_path / "success"
    success_dir.mkdir()
    success_state = {
        "pid": 1,
        "workdir": str(success_dir),
        "result_path": str(success_dir / "result.json"),
        "stdout_path": str(success_dir / "stdout.log"),
        "stderr_path": str(success_dir / "stderr.log"),
    }
    (success_dir / "result.json").write_text(
        json.dumps({"lifecycle": "succeeded", "error": None, "state": None, "dag_id": "a" * 64})
    )
    assert ScriptExecutor().poll(
        cache_key="ck",
        execution_id="exec",
        runnable={"target": {"uri": "script"}, "kwargs": {}, "adapter": "dml-local-adapter", "sub": None},
        state=dict(success_state),
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/scratch",
    )["lifecycle"] == "succeeded"

    bad_dir = tmp_path / "bad"
    bad_dir.mkdir()
    bad_state = {
        "pid": 1,
        "workdir": str(bad_dir),
        "result_path": str(bad_dir / "result.json"),
        "stdout_path": str(bad_dir / "stdout.log"),
        "stderr_path": str(bad_dir / "stderr.log"),
    }
    (bad_dir / "result.json").write_text("not-json")
    assert ScriptExecutor().poll(
        cache_key="ck",
        execution_id="exec",
        runnable={"target": {"uri": "script"}, "kwargs": {}, "adapter": "dml-local-adapter", "sub": None},
        state=bad_state,
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/scratch",
    )["lifecycle"] == "failed"

    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    empty_state = {
        "pid": 1,
        "workdir": str(empty_dir),
        "result_path": str(empty_dir / "result.json"),
        "stdout_path": str(empty_dir / "stdout.log"),
        "stderr_path": str(empty_dir / "stderr.log"),
    }
    assert "without result" in ScriptExecutor().poll(
        cache_key="ck",
        execution_id="exec",
        runnable={"target": {"uri": "script"}, "kwargs": {}, "adapter": "dml-local-adapter", "sub": None},
        state=empty_state,
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/scratch",
    )["error"]


def test_script_worker_dag_creation_uses_cache_key_and_execution_id(monkeypatch, tmp_path):
    calls = {}
    tmpdml = SimpleNamespace(_config=SimpleNamespace(project_home=str(tmp_path)))

    @contextmanager
    def fake_temporary(**kwargs):
        calls["temporary"] = kwargs
        yield tmpdml

    def fake_new(**kwargs):
        calls["new"] = kwargs
        raise RuntimeError("stop after DAG creation")

    monkeypatch.setattr(script_mod.dml, "temporary", fake_temporary)
    monkeypatch.setattr(script_mod.dml, "new", fake_new)
    result = script_mod.run_payload(execution_id="exec-1", cache_key="cache-1", remote_root="s3://bucket/root")
    assert calls["new"] == {"dml": tmpdml, "cache_key": "cache-1", "execution_id": "exec-1"}
    assert result["lifecycle"] == "failed"


def test_contrib_script_006__run_payload_uses_prepop_and_script_uri_from_runnable(monkeypatch, tmp_path):
    calls = {"put": []}
    tmpdml = SimpleNamespace(_config=SimpleNamespace(project_home=str(tmp_path)))

    class FakeDag:
        def __init__(self):
            self.argv = [
                SimpleNamespace(
                    value=lambda: SimpleNamespace(
                        innermost=lambda: SimpleNamespace(
                            kwargs={
                                "prepop": {"seed": 7},
                                "script_uri": "s3://bucket/script.py",
                                "fn_name": "fn",
                            }
                        )
                    )
                ),
                "arg-node",
            ]
            self.ref = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def put(self, value, name=None):
            calls["put"].append((name, value))

        def commit(self, output):
            calls["commit"] = output
            self.ref = SimpleNamespace(id=lambda: "d" * 64)

    @contextmanager
    def fake_temporary(**kwargs):
        yield tmpdml

    def fake_new(**kwargs):
        calls["new"] = kwargs
        return FakeDag()

    class FakeStore:
        def get(self, uri):
            calls["script_uri"] = uri
            return b"def fn(dag, arg):\n    return f'result:{arg}'\n"

    monkeypatch.setattr(script_mod.dml, "temporary", fake_temporary)
    monkeypatch.setattr(script_mod.dml, "new", fake_new)
    monkeypatch.setattr(script_mod, "S3Store", lambda: FakeStore())

    result = script_mod.run_payload(execution_id="exec-1", cache_key="cache-1", remote_root="s3://bucket/root")

    assert result == {"lifecycle": "succeeded", "state": None, "error": None, "dag_id": "d" * 64}
    assert calls["script_uri"] == "s3://bucket/script.py"
    assert calls["put"] == [("seed", 7)]
    assert calls["commit"] == "result:arg-node"
