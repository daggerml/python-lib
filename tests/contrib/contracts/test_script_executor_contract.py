from __future__ import annotations

import json
import logging
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

import daggerml.contrib.executors.script as script_mod
from daggerml import Runnable, Uri
from daggerml.api import DmlRepoError
from daggerml.contrib import api
from daggerml.contrib.executors.script import ScriptExecutor


def _run_worker_script(monkeypatch, tmp_path: Path, source: str):
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

    class FakeStore:
        def get(self, uri):
            calls["script_uri"] = uri
            return source.encode("utf-8")

    monkeypatch.setattr(script_mod.dml, "temporary", fake_temporary)
    monkeypatch.setattr(script_mod.dml, "new", lambda **kwargs: FakeDag())
    monkeypatch.setattr(script_mod, "S3Store", lambda: FakeStore())
    result = script_mod.run_payload(execution_id="exec-1", cache_key="cache-1", remote_root="s3://bucket/root")
    return result, calls


def test_script_kwargs_capture_fn_name_and_require_parameter():
    def fn(dag, x):
        return x.value()

    kwargs, script = ScriptExecutor._script_kwargs({"fn": fn, "prepop": {}})
    assert kwargs == {"prepop": {}, "fn_name": "fn"}
    assert "def fn" in script

    def bad():
        return None

    with pytest.raises(DmlRepoError, match="at least one parameter"):
        ScriptExecutor._script_kwargs({"fn": bad})


def test_script_kwargs_normalize_tags():
    def fn(dag):
        return 1

    kwargs, _ = ScriptExecutor._script_kwargs({"fn": fn, "tags": ["research.v0", "candidate", "candidate"]})

    assert kwargs["tags"] == ["candidate", "research.v0"]
    with pytest.raises(DmlRepoError, match="tags must be a list of strings"):
        ScriptExecutor._script_kwargs({"fn": fn, "tags": "candidate"})


def test_funkify_captures_script_before_staging(monkeypatch):
    def helper(value):
        return value + 1

    def fn(dag, value):
        return helper(value)

    delayed = api.funkify(fn, extra_objs=[helper], post_lines=["CAPTURED = True"])
    captured = delayed.kwargs["script"]
    assert delayed.kwargs["fn_name"] == "fn"
    assert "def helper" in captured
    assert "CAPTURED = True" in captured

    def fail_if_rendered(cls, fn, extra_objs, post_lines):
        raise AssertionError("source was inspected after funkification")

    monkeypatch.setattr(ScriptExecutor, "_render_script", classmethod(fail_if_rendered))
    resolved, staged = ScriptExecutor._script_kwargs(delayed.kwargs)
    assert resolved["fn_name"] == "fn"
    assert staged == captured


def test_funkify_fails_immediately_when_source_is_unavailable(monkeypatch):
    def fn(dag):
        return 1

    def unavailable(_obj):
        raise OSError("source cache is empty")

    monkeypatch.setattr(script_mod.inspect, "getsource", unavailable)
    with pytest.raises(DmlRepoError, match="Failed to serialize object source: source cache is empty"):
        api.funkify(fn)


def test_eager_capture_does_not_include_authoring_globals():
    authoring_secret = "must-not-be-captured"

    def fn(dag):
        return authoring_secret

    delayed = api.funkify(fn)

    assert "must-not-be-captured" not in delayed.kwargs["script"]
    assert "authoring_secret" in delayed.kwargs["script"]


def test_rendered_source_rejects_pathological_wrapped_functions():
    def fn(dag):
        return 1

    fn.__wrapped__ = fn
    with pytest.raises(ValueError, match="wrapper loop"):
        ScriptExecutor._render_script(fn, extra_objs=[], post_lines=[])


def test_resolve_runnable_rejects_sub_and_writes_script_to_s3(monkeypatch):
    seen = {}

    class FakeStore:
        def put(self, *, data, suffix):
            seen.update({"data": data.decode("utf-8"), "suffix": suffix})
            return Uri("s3://bucket/script.py")

    monkeypatch.setattr(script_mod, "S3Store", lambda: FakeStore())

    def fn(dag):
        return 1

    runnable = ScriptExecutor.resolve_runnable("script", {"fn": fn}, None)
    assert runnable.kwargs["script_uri"] == Uri("s3://bucket/script.py")
    assert runnable.kwargs["fn_name"] == "fn"
    assert seen["suffix"] == ".py"

    with pytest.raises(DmlRepoError, match="does not accept sub runnable"):
        ScriptExecutor.resolve_runnable("script", {"fn": fn}, Runnable(target=Uri("inner"), kwargs={}, adapter="x"))


def test_start_returns_durable_running_state(monkeypatch):
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
    assert result["status"] == "retry"
    assert result["state"]["pid"] == 123
    assert "workdir" in result["state"]


def test_poll_handles_terminal_malformed_and_no_result_paths(monkeypatch, tmp_path):
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
        json.dumps({"status": "succeeded", "error": None, "dag_id": "a" * 64})
    )
    success = ScriptExecutor().poll(
        cache_key="ck",
        execution_id="exec",
        runnable={"target": {"uri": "script"}, "kwargs": {}, "adapter": "dml-local-adapter", "sub": None},
        state=dict(success_state),
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/scratch",
    )
    assert success["status"] == "success"
    assert success["state"] == success_state
    assert success_dir.exists()

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
    assert (
        ScriptExecutor().poll(
            cache_key="ck",
            execution_id="exec",
            runnable={"target": {"uri": "script"}, "kwargs": {}, "adapter": "dml-local-adapter", "sub": None},
            state=bad_state,
            remote={"root": "s3://bucket/root"},
            scratch_uri="s3://bucket/scratch",
        )["status"]
        == "failure"
    )

    cleanup = ScriptExecutor().cleanup(
        "ck", "exec", {}, success_state, {"root": "s3://bucket/root"}, "scratch", "dag:x"
    )
    assert cleanup["status"] == "success"
    assert not success_dir.exists()

    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    empty_state = {
        "pid": 1,
        "workdir": str(empty_dir),
        "result_path": str(empty_dir / "result.json"),
        "stdout_path": str(empty_dir / "stdout.log"),
        "stderr_path": str(empty_dir / "stderr.log"),
    }
    assert (
        "without result"
        in ScriptExecutor().poll(
            cache_key="ck",
            execution_id="exec",
            runnable={"target": {"uri": "script"}, "kwargs": {}, "adapter": "dml-local-adapter", "sub": None},
            state=empty_state,
            remote={"root": "s3://bucket/root"},
            scratch_uri="s3://bucket/scratch",
        )["error"]
    )


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
    assert result["status"] == "failed"
    assert "stop after DAG creation" in result["error"]
    assert "Traceback" in result["error"]


def test_script_worker_dag_creation_passes_declared_tags(monkeypatch, tmp_path):
    calls = {}
    tmpdml = SimpleNamespace(_config=SimpleNamespace(project_home=str(tmp_path)))

    @contextmanager
    def fake_temporary(**kwargs):
        yield tmpdml

    def fake_new(**kwargs):
        calls["new"] = kwargs
        raise RuntimeError("stop after DAG creation")

    monkeypatch.setattr(script_mod.dml, "temporary", fake_temporary)
    monkeypatch.setattr(script_mod.dml, "new", fake_new)

    script_mod.run_payload(
        execution_id="exec-1",
        cache_key="cache-1",
        remote_root="s3://bucket/root",
        tags=["candidate", "research.v0"],
    )

    assert calls["new"] == {
        "dml": tmpdml,
        "cache_key": "cache-1",
        "execution_id": "exec-1",
        "tags": ["candidate", "research.v0"],
    }


def test_cancel_without_launch_state_is_still_cancelled():
    result = ScriptExecutor().cancel(
        cache_key="ck",
        execution_id="exec",
        runnable={"target": {"uri": "script"}, "kwargs": {}, "adapter": "dml-local-adapter", "sub": None},
        state=None,
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/scratch",
        cancel_requested_by="user",
    )

    assert result == {"status": "cancelled", "error": None, "state": {}}


def test_contrib_script_cancel_reports_permission_failure(monkeypatch):
    monkeypatch.setattr(
        "daggerml.contrib.executors.script.os.killpg",
        lambda *_: (_ for _ in ()).throw(PermissionError("denied")),
    )

    result = ScriptExecutor().cancel(
        cache_key="ck",
        execution_id="exec",
        runnable={},
        state={"pid": 123},
        remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/scratch",
        cancel_requested_by="user",
    )

    assert result["status"] == "failure"
    assert "denied" in result["error"]


def test_script_cleanup_preserves_workdir_if_supervisor_cannot_be_terminated(monkeypatch, tmp_path):
    workdir = tmp_path / "scratch"
    workdir.mkdir()
    monkeypatch.setattr(script_mod.os, "waitpid", lambda pid, flags: (0, 0))
    monkeypatch.setattr(
        script_mod.os, "killpg", lambda pid, sig: (_ for _ in ()).throw(PermissionError("denied"))
    )

    result = ScriptExecutor().cleanup("ck", "exec", {}, {"pid": 123, "workdir": str(workdir)}, {}, "scratch", "dag:x")

    assert result["status"] == "failure"
    assert "denied" in result["error"]
    assert workdir.exists()


def test_run_payload_uses_prepop_and_script_uri_from_runnable(monkeypatch, tmp_path):
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

    assert result == {"status": "succeeded", "error": None, "dag_id": "d" * 64}
    assert calls["script_uri"] == "s3://bucket/script.py"
    assert calls["put"] == [("seed", 7)]
    assert calls["commit"] == "result:arg-node"


def test_worker_executes_file_backed_live_module(monkeypatch, tmp_path):
    source = """
import sys

self_module = __import__("_daggerml_live")

def fn(dag, arg):
    return {
        "arg": arg,
        "file": __file__,
        "function_module": fn.__module__,
        "loader": type(__loader__).__name__,
        "module": __name__,
        "package": __package__,
        "self_import": self_module is sys.modules[__name__],
        "spec_name": __spec__.name,
    }
"""

    try:
        result, calls = _run_worker_script(monkeypatch, tmp_path, source)

        assert result == {"status": "succeeded", "error": None, "dag_id": "d" * 64}
        assert calls["commit"] == {
            "arg": "arg-node",
            "file": str(tmp_path / "_daggerml_live.py"),
            "function_module": "_daggerml_live",
            "loader": "SourceFileLoader",
            "module": "_daggerml_live",
            "package": "",
            "self_import": True,
            "spec_name": "_daggerml_live",
        }
        assert (tmp_path / "_daggerml_live.py").read_text() == source
    finally:
        sys.modules.pop("_daggerml_live", None)


def test_failed_live_module_reports_source_and_cleans_sys_modules(monkeypatch, tmp_path):
    result, _ = _run_worker_script(monkeypatch, tmp_path, 'raise RuntimeError("module boom")\n')

    assert result["status"] == "failed"
    assert "module boom" in result["error"]
    assert str(tmp_path / "_daggerml_live.py") in result["error"]
    assert "_daggerml_live" not in sys.modules


def test_live_logger_writes_debug_once_without_changing_other_loggers(
    monkeypatch, tmp_path, capsys
):
    dependency_logger = logging.getLogger("dependency-under-test")
    dependency_handler = logging.NullHandler()
    dependency_logger.handlers = [dependency_handler]
    dependency_logger.setLevel(logging.INFO)
    dependency_logger.propagate = True
    source = """
import logging

logger.debug("injected-debug")
logging.getLogger(__name__).debug("module-debug")

def fn(dag, arg):
    return arg
"""

    try:
        result, _ = _run_worker_script(monkeypatch, tmp_path, source)
        captured = capsys.readouterr()

        assert result["status"] == "succeeded"
        assert captured.err.count("injected-debug") == 1
        assert captured.err.count("module-debug") == 1
        assert dependency_logger.handlers == [dependency_handler]
        assert dependency_logger.level == logging.INFO
        assert dependency_logger.propagate is True
    finally:
        logging.getLogger("_daggerml_live").handlers.clear()
        sys.modules.pop("_daggerml_live", None)


def test_funk_failure_reports_live_module_source_line(monkeypatch, tmp_path):
    source = """
def fn(dag, arg):
    raise ValueError("funk boom")
"""

    try:
        result, _ = _run_worker_script(monkeypatch, tmp_path, source)

        assert result["status"] == "failed"
        assert "funk boom" in result["error"]
        assert str(tmp_path / "_daggerml_live.py") in result["error"]
        assert 'raise ValueError("funk boom")' in result["error"]
    finally:
        sys.modules.pop("_daggerml_live", None)
