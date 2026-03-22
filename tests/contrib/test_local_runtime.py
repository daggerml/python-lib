from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, cast

import pytest

from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib import adapter_registry as areg
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib.adapters import AdapterBase, LocalAdapter
from daggerml.contrib.executor_state import LocalState
from daggerml.contrib.executors import ExecutorBase, ScriptExecutor
from daggerml.contrib.s3 import S3Store


@pytest.fixture(autouse=True)
def _reset_registries(tmp_path, monkeypatch):
    areg._reset_for_tests()
    ereg._reset_for_tests()
    monkeypatch.setenv("DML_FN_CACHE_DIR", str(tmp_path / "state"))

    class EchoExecutor:
        name = "echo"
        adapter = "local"
        state_class = LocalState

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @staticmethod
        def start(*, runnable, argv_ptr, cache_key, remote, state=None):
            state.put_if_absent(state.init_record(status="succeeded", error=None))
            return {"status": "succeeded", "error": None}

        @staticmethod
        def poll(*, state=None):
            current = state.get() or {"status": "running", "error": None}
            return {"status": current.get("status"), "error": current.get("error")}

        @staticmethod
        def gc(*, state=None):
            return None

    ereg.register_executor(EchoExecutor)
    ereg.register_executor(ScriptExecutor)
    yield
    areg._reset_for_tests()
    ereg._reset_for_tests()


def _remote() -> dict[str, str]:
    return {"root": os.environ["DML_REMOTE_ROOT"], "cache": "test-cache"}


def _mk_argv_ptr(*args: Any, argv0: Any | None = None) -> str:
    from daggerml import Dml

    with Dml.temporary() as dml:
        dag = dml.new("argv-src", "argv-src")
        index_ref = dag._require_index_ref()
        head = argv0 if argv0 is not None else Runnable(target=Uri("daggerml:list"), kwargs={}, adapter="")
        fn_ref = dml.index.put_literal(index_ref, head)
        arg_refs = [dml.index.put_literal(index_ref, value) for value in args]
        with dml.index._tx(readonly=False) as txn:
            argv_ref = dml.index._prepare_fn(index_ref, [fn_ref, *arg_refs], {}, txn)
        return dml.index._remote_ops().put_ptr(argv_ref)


def _poll_until_terminal(*, runnable: Runnable, argv_ptr: str, cache_key: str) -> dict[str, Any]:
    for _ in range(200):
        result = LocalAdapter.send(runnable=runnable, argv_ptr=argv_ptr, cache_key=cache_key, remote=_remote())
        if result["status"] in {"succeeded", "failed"}:
            return cast(dict[str, Any], result)
        time.sleep(0.01)
    pytest.fail("script executor did not reach terminal state")


def _mk_script_runnable(script: str, *, fn_name: str = "fn", call_kwargs: dict[str, Any] | None = None) -> Runnable:
    uri = S3Store().put(data=script.encode("utf-8"), suffix=".py")
    return Runnable(
        target=Uri("script"),
        adapter="dml-local-adapter",
        kwargs={
            "__dml_script_exec__": {"prepop": {}, "fn_name": fn_name, "script_uri": uri.uri},
            **dict(call_kwargs or {}),
        },
    )


def test_local_adapter_resolve_runnable_shape():
    result = LocalAdapter.resolve_runnable("echo", {"x": 1}, None)
    assert isinstance(result, Runnable)
    assert isinstance(result.target, Uri)
    assert result.target.uri == "echo"
    assert result.kwargs == {"x": 1}
    assert result.adapter == "dml-local-adapter"


def test_local_adapter_script_resolve_runnable_derives_call_kwargs_and_script():
    def fn(dag, x, y=2, *, z=3):
        return x + y + z

    result = LocalAdapter.resolve_runnable("script", {"fn": fn, "prepop": {}}, None)
    assert isinstance(result, Runnable)
    assert result.target.uri == "script"
    meta = result.kwargs["__dml_script_exec__"]
    assert isinstance(meta["script_uri"], str)
    assert meta["script_uri"].startswith("s3://test-bucket/test-prefix/data/")
    assert meta["script_uri"].endswith(".py")
    assert result.kwargs["y"] == 2
    assert result.kwargs["z"] == 3
    assert meta["fn_name"] == "fn"
    assert "script" not in result.kwargs
    assert "required_positional_count" not in result.kwargs


def test_local_adapter_script_resolve_runnable_rejects_unknown_kwargs():
    def fn(dag):
        return None

    with pytest.raises(DmlRepoError, match="Unknown script executor kwargs"):
        LocalAdapter.resolve_runnable("script", {"fn": fn, "call_kwargs": {}}, None)


def test_local_adapter_script_resolve_runnable_rejects_no_dag_param():
    def fn():
        return None

    with pytest.raises(DmlRepoError, match="must include first 'dag' parameter"):
        LocalAdapter.resolve_runnable("script", {"fn": fn}, None)


def test_local_adapter_script_resolve_runnable_requires_dag_as_first_param():
    def fn(x, dag):
        return x

    with pytest.raises(DmlRepoError, match="must include first 'dag' parameter"):
        LocalAdapter.resolve_runnable("script", {"fn": fn}, None)


def test_local_adapter_script_resolve_runnable_requires_global_fn_definition():
    def _mk_fn():
        return lambda dag: None

    fn = _mk_fn()
    with pytest.raises(DmlRepoError, match="not globally defined"):
        resp = LocalAdapter.resolve_runnable("script", {"fn": fn}, None)
        assert resp.kwargs["script"] == ""  # for better error visibility


def test_local_adapter_script_resolve_runnable_rejects_sub_runnable():
    def fn(dag):
        return None

    sub = Runnable(target=Uri("inner"), adapter="dml-local-adapter", kwargs={}, sub=None)
    with pytest.raises(DmlRepoError, match="does not accept sub runnable"):
        LocalAdapter.resolve_runnable("script", {"fn": fn}, sub)


def test_script_executor_run_executes_script_and_reaches_success():
    script = "\n".join(["def fn(dag, x, y=2):", "    return x.value() + y.value()", ""])
    runnable = _mk_script_runnable(script, call_kwargs={"y": 2})
    kickoff = LocalAdapter.send(
        runnable=runnable, argv_ptr=_mk_argv_ptr(3, argv0=runnable), cache_key="ck-run", remote=_remote()
    )
    assert kickoff == {"status": "running", "error": None}
    result = _poll_until_terminal(runnable=runnable, argv_ptr=_mk_argv_ptr(3, argv0=runnable), cache_key="ck-run")
    assert result["status"] == "succeeded"
    assert result["error"] is None


def test_script_executor_run_extracts_innermost_prepop():
    script = "\n".join(["def fn(dag):", "    return 'seed' in dag.keys()", ""])
    runnable = _mk_script_runnable(script)
    meta = cast(dict[str, Any], runnable.kwargs["__dml_script_exec__"])
    inner = Runnable(
        target=Uri("inner"),
        adapter="dml-local-adapter",
        kwargs={
            "__dml_script_exec__": {
                "prepop": {"seed": 9},
                "fn_name": cast(str, meta["fn_name"]),
                "script_uri": cast(str, meta["script_uri"]),
            }
        },
        sub=None,
    )
    outer = Runnable(target=Uri("outer"), adapter="dml-local-adapter", kwargs={}, sub=inner)

    result = _poll_until_terminal(runnable=runnable, argv_ptr=_mk_argv_ptr(argv0=outer), cache_key="ck-prepop")
    assert result["status"] == "succeeded"


def test_script_executor_start_returns_running_with_handle():
    script = "\n".join(["import time", "def fn(dag):", "    time.sleep(0.2)", "    return 1", ""])
    runnable = _mk_script_runnable(script)
    cache_key = "ck-start-handle"
    with ScriptExecutor.state_class(cache_key).lock() as state:
        assert state is not None
        result = ScriptExecutor.start(
            runnable=runnable,
            argv_ptr=_mk_argv_ptr(argv0=runnable),
            cache_key=cache_key,
            remote=_remote(),
            state=state,
        )
    assert result == {"status": "running", "error": None}
    record = LocalState(cache_key).get()
    assert isinstance(record, dict)
    script_meta = cast(dict[str, Any], cast(dict[str, Any], record.get("metadata")).get("script"))
    assert isinstance(script_meta.get("pid"), int)
    assert isinstance(script_meta.get("result_path"), str)
    assert isinstance(script_meta.get("stdout_path"), str)
    assert isinstance(script_meta.get("stderr_path"), str)
    assert Path(cast(str, script_meta["stdout_path"])).exists()
    assert Path(cast(str, script_meta["stderr_path"])).exists()
    with ScriptExecutor.state_class(cache_key).lock() as state:
        assert state is not None
        ScriptExecutor.gc(state=state)


def test_script_executor_gc_cancels_running():
    script = "\n".join(["import time", "def fn(dag):", "    time.sleep(1.0)", "    return 1", ""])
    runnable = _mk_script_runnable(script)
    cache_key = "ck-gc-cancel"
    with ScriptExecutor.state_class(cache_key).lock() as state:
        assert state is not None
        start = ScriptExecutor.start(
            runnable=runnable,
            argv_ptr=_mk_argv_ptr(argv0=runnable),
            cache_key=cache_key,
            remote=_remote(),
            state=state,
        )
    assert start == {"status": "running", "error": None}
    with ScriptExecutor.state_class(cache_key).lock() as state:
        assert state is not None
        ScriptExecutor.gc(state=state)
    assert ScriptExecutor.state_class(cache_key).get() is None


def test_script_executor_run_returns_running_during_inflight_poll():
    script = "\n".join(["import time", "def fn(dag):", "    time.sleep(0.2)", "    return 1", ""])
    runnable = _mk_script_runnable(script)
    first = LocalAdapter.send(
        runnable=runnable, argv_ptr=_mk_argv_ptr(argv0=runnable), cache_key="ck-inflight", remote=_remote()
    )
    second = LocalAdapter.send(
        runnable=runnable, argv_ptr=_mk_argv_ptr(argv0=runnable), cache_key="ck-inflight", remote=_remote()
    )
    assert first == {"status": "running", "error": None}
    assert second == {"status": "running", "error": None}
    terminal = _poll_until_terminal(runnable=runnable, argv_ptr=_mk_argv_ptr(argv0=runnable), cache_key="ck-inflight")
    assert terminal["status"] == "succeeded"


def test_local_adapter_uses_comms_backend_for_outer_state(tmp_path):
    script = "\n".join(["import time", "def fn(dag):", "    time.sleep(0.5)", "    return 1", ""])
    runnable = _mk_script_runnable(script)
    cache_key = "ck-comms-propagation"
    root_dir = tmp_path / "state-custom"

    in_file = tmp_path / "input.json"
    out_file = tmp_path / "output.json"
    in_file.write_bytes(
        LocalAdapter._dump_payload(
            runnable=runnable,
            argv_ptr=_mk_argv_ptr(argv0=runnable),
            cache_key=cache_key,
            remote=_remote(),
            comms={"kind": "local", "spec": {"cache_dir": str(root_dir)}},
        )
    )
    assert LocalAdapter.cli(["-i", str(in_file), "-o", str(out_file)]) == 0
    kickoff = json.loads(out_file.read_text())
    assert kickoff == {"status": "running", "error": None}

    record = LocalState(cache_key, cache_dir=str(root_dir)).get()
    assert isinstance(record, dict)
    assert record["status"] == "running"
    assert isinstance(record.get("heartbeat_ts"), float)


def test_script_executor_run_records_good_state_on_runtime_exception():
    script = "\n".join(["def fn(dag):", "    raise RuntimeError('boom')", ""])
    runnable = _mk_script_runnable(script)
    result = _poll_until_terminal(runnable=runnable, argv_ptr=_mk_argv_ptr(argv0=runnable), cache_key="ck-fail")
    assert result["status"] == "succeeded"


def test_local_adapter_resolve_runnable_rejects_executor_for_other_adapter():
    class ForeignExecutor:
        name = "foreign"
        adapter = "lambda"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri("foreign"), adapter="x", kwargs={}, sub=None)

        state_class = LocalState

        @staticmethod
        def start(*, runnable, argv_ptr, cache_key, remote, state=None):
            return {"status": "running", "error": None}

        @staticmethod
        def poll(*, state=None):
            return {"status": "running", "error": None}

        @staticmethod
        def gc(*, state=None):
            return None

    ereg.register_executor(ForeignExecutor)
    with pytest.raises(DmlRepoError, match="is not registered for adapter 'local'"):
        LocalAdapter.resolve_runnable("foreign", {}, None)


def test_local_adapter_dispatches_to_executor_lifecycle():
    seen: dict[str, Any] = {}

    class DispatchExecutorForTest:
        name = "dispatch-test"
        adapter = "local"
        state_class = LocalState

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @staticmethod
        def start(*, runnable, argv_ptr, cache_key, remote, state=None):
            seen["runnable"] = runnable
            seen["argv_ptr"] = argv_ptr
            seen["cache_key"] = cache_key
            seen["remote"] = remote
            state.put_if_absent(state.init_record(status="succeeded", error=None))
            return {"status": "succeeded", "error": None}

        @staticmethod
        def poll(*, state=None):
            current = state.get() or {"status": "running", "error": None}
            return {"status": current.get("status"), "error": current.get("error")}

        @staticmethod
        def gc(*, state=None):
            seen["gc_called"] = True
            return None

    ereg.register_executor(DispatchExecutorForTest)
    runnable = Runnable(target=Uri("dispatch-test"), adapter="dml-local-adapter", kwargs={})
    payload = LocalAdapter.send(runnable=runnable, argv_ptr=_mk_argv_ptr("a"), cache_key="ck", remote=_remote())
    payload = cast(dict[str, Any], payload)
    assert payload == {"status": "succeeded", "error": None}
    assert isinstance(seen["argv_ptr"], str)
    assert seen["cache_key"] == "ck"
    assert seen["gc_called"] is True


def test_local_adapter_unknown_executor_fails_deterministically():
    runnable = Runnable(target=Uri("missing"), adapter="dml-local-adapter", kwargs={})
    with pytest.raises(DmlRepoError, match="Executor 'missing' is not registered for adapter 'local'"):
        LocalAdapter.send(runnable=runnable, argv_ptr=_mk_argv_ptr(), cache_key="ck", remote=_remote())


def test_registered_executor_minimal_run_contract():
    runnable = Runnable(target=Uri("echo"), adapter="dml-local-adapter", kwargs={})
    result = LocalAdapter.send(runnable=runnable, argv_ptr=_mk_argv_ptr(1), cache_key="ck", remote=_remote())
    result = cast(dict[str, Any], result)
    assert result == {"status": "succeeded", "error": None}


def test_local_adapter_send_returns_executor_payload_as_emitted():
    class RunningExecutor:
        name = "running"
        adapter = "local"
        state_class = LocalState

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @staticmethod
        def start(*, runnable, argv_ptr, cache_key, remote, state=None):
            state.put_if_absent(state.init_record(status="running", error=None))
            return {"status": "running", "error": None}

        @staticmethod
        def poll(*, state=None):
            return {"status": "running", "error": None}

        @staticmethod
        def gc(*, state=None):
            return None

    ereg.register_executor(RunningExecutor)
    runnable = Runnable(target=Uri("running"), adapter="dml-local-adapter", kwargs={})
    result = LocalAdapter.send(runnable=runnable, argv_ptr=_mk_argv_ptr(), cache_key="ck", remote=_remote())
    result = cast(dict[str, Any], result)
    assert result == {"status": "running", "error": None}


def test_local_adapter_returns_canceled_from_canonical_state_status():
    runnable = Runnable(target=Uri("echo"), adapter="dml-local-adapter", kwargs={})
    cache_key = "ck-canceled-status"
    with LocalState(cache_key).lock() as state:
        assert state is not None
        state.update(state.init_record(status="canceled", error=None))

    result = LocalAdapter.send(runnable=runnable, argv_ptr=_mk_argv_ptr(), cache_key=cache_key, remote=_remote())
    result = cast(dict[str, Any], result)
    assert result == {"status": "canceled", "error": None}


def test_local_adapter_send_rejects_non_contract_payload():
    class BadExecutor:
        name = "bad"
        adapter = "local"
        state_class = LocalState

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @staticmethod
        def start(*, runnable, argv_ptr, cache_key, remote, state=None):
            return {"status": "running", "error": None, "extra": 1}

        @staticmethod
        def poll(*, state=None):
            return {"status": "running", "error": None}

        @staticmethod
        def gc(*, state=None):
            return None

    ereg.register_executor(BadExecutor)
    runnable = Runnable(target=Uri("bad"), adapter="dml-local-adapter", kwargs={})
    with pytest.raises(DmlRepoError, match="Adapter output keys must be exactly"):
        LocalAdapter.send(runnable=runnable, argv_ptr=_mk_argv_ptr(), cache_key="ck", remote=_remote())


def test_executor_base_start_not_implemented():
    with pytest.raises(NotImplementedError, match="Executor start method is not implemented"):
        ExecutorBase.start(runnable=None, argv_ptr=_mk_argv_ptr(), cache_key="ck", remote=_remote(), state=None)


def test_executor_base_poll_not_implemented():
    with pytest.raises(NotImplementedError, match="Executor poll method is not implemented"):
        ExecutorBase.poll(state=None)


def test_executor_base_gc_not_implemented():
    with pytest.raises(NotImplementedError, match="Executor gc method is not implemented"):
        ExecutorBase.gc(state=None)


def test_adapter_base_cli_reads_stdin_and_writes_stdout(capsys):
    class DummyAdapter(AdapterBase):
        @classmethod
        def send(cls, *, runnable, argv_ptr, cache_key, remote):
            return {"status": "succeeded", "error": None}

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "sys.stdin.read",
            lambda: DummyAdapter._dump_payload(
                runnable=Runnable(target=Uri("x"), adapter="dummy", kwargs={}),
                argv_ptr="ptr",
                cache_key="ck",
                remote=_remote(),
            ).decode("utf-8"),
        )
        exit_code = DummyAdapter.cli([])

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out.strip()) == {"status": "succeeded", "error": None}


def test_adapter_base_cli_reads_and_writes_files(tmp_path):
    class DummyAdapter(AdapterBase):
        @classmethod
        def send(cls, *, runnable, argv_ptr, cache_key, remote):
            return {"status": "succeeded", "error": None}

    in_file = tmp_path / "in.json"
    out_file = tmp_path / "out.json"
    in_file.write_bytes(
        DummyAdapter._dump_payload(
            runnable=Runnable(target=Uri("x"), adapter="dummy", kwargs={}),
            argv_ptr="ptr",
            cache_key="ck",
            remote=_remote(),
        )
    )

    exit_code = DummyAdapter.cli(["-i", str(in_file), "-o", str(out_file)])
    assert exit_code == 0
    assert json.loads(out_file.read_text()) == {"status": "succeeded", "error": None}


def test_local_state_lock_put_update_delete(tmp_path):
    with LocalState("abc", cache_dir=str(tmp_path)).lock() as state:
        assert state is not None
        assert state.get() is None
        assert state.put_if_absent(state.init_record(status="running", error=None)) is True
        assert state.put_if_absent(state.init_record(status="pending", error=None)) is False
        assert cast(dict[str, Any], state.get())["status"] == "running"
        state.update(state.update_status(status="succeeded", error=None))
        current = cast(dict[str, Any], state.get())
        assert current["status"] == "succeeded"
        state.delete()
        assert state.get() is None
