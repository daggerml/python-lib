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
from daggerml.contrib.executors import ScriptExecutor
from daggerml.contrib.s3 import S3Store

pytestmark = pytest.mark.slow


@pytest.fixture(autouse=True)
def _reset_registries(tmp_path, monkeypatch):
    areg._reset_for_tests()
    ereg._reset_for_tests()
    monkeypatch.setenv("DML_TEST_FN_STATE_DIR", str(tmp_path / "state"))

    class EchoExecutor:
        name = "echo"
        adapter = "local"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @classmethod
        def handle(cls, *, cache_key, execution_id, state, runnable, argv_ptr, remote):
            return {"status": "succeeded", "error": None, "dag_id": "a" * 64}

    ereg.register_executor(EchoExecutor)
    ereg.register_executor(ScriptExecutor)
    yield
    areg._reset_for_tests()
    ereg._reset_for_tests()


def _remote() -> dict[str, str]:
    return {"root": os.environ["DML_REMOTE_URI"]}


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
        return dml.index._remote_ops().put_ref_manifest(argv_ref)


def _poll_until_terminal(*, runnable: Runnable, argv_ptr: str, cache_key: str) -> dict[str, Any]:
    execution_id = f"exec-{cache_key}"
    state: dict[str, Any] | None = None
    for _ in range(200):
        result = LocalAdapter.send(
            runnable=runnable,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
            execution_id=execution_id,
            remote=_remote(),
            state=state,
        )
        if state is None and result.get("status") == "running":
            state = cast(dict[str, Any], result.get("state"))
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


@pytest.mark.parametrize(
    "script,call_kwargs,inject_prepop,resume_once,expected_terminal",
    [
        pytest.param(
            "\n".join(["def fn(dag, x, y=2):", "    return x.value() + y.value()", ""]),
            {"y": 2},
            False,
            False,
            "succeeded",
            id="LRT-LFC-001:kickoff-running-then-terminal-success",
        ),
        pytest.param(
            "\n".join(["def fn(dag):", "    return 'seed' in dag.keys()", ""]),
            None,
            True,
            False,
            "succeeded",
            id="LRT-LFC-002:terminal-success-with-innermost-prepop",
        ),
        pytest.param(
            "\n".join(["import time", "def fn(dag):", "    time.sleep(0.2)", "    return 1", ""]),
            None,
            False,
            True,
            "succeeded",
            id="LRT-LFC-003:resume-poll-remains-running-before-terminal",
        ),
        pytest.param(
            "\n".join(["def fn(dag):", "    raise RuntimeError('boom')", ""]),
            None,
            False,
            False,
            "succeeded",
            id="LRT-LFC-004:runtime-exception-path-terminal-envelope",
        ),
    ],
)
def test_script_executor_lifecycle_stage_matrix_LRT_LFC_001_to_LRT_LFC_004(
    script, call_kwargs, inject_prepop, resume_once, expected_terminal
):
    runnable = _mk_script_runnable(script, call_kwargs=call_kwargs)

    if inject_prepop:
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
        argv_ptr = _mk_argv_ptr(argv0=outer)
    else:
        argv_ptr = _mk_argv_ptr(3, argv0=runnable) if call_kwargs else _mk_argv_ptr(argv0=runnable)

    cache_key = f"ck-stage-{expected_terminal}-{'resume' if resume_once else 'kickoff'}-{'prepop' if inject_prepop else 'plain'}"
    kickoff = LocalAdapter.send(
        runnable=runnable,
        argv_ptr=argv_ptr,
        cache_key=cache_key,
        execution_id=f"exec-{cache_key}",
        remote=_remote(),
        state=None,
    )
    assert kickoff["status"] == "running"

    if resume_once:
        resumed = LocalAdapter.send(
            runnable=runnable,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
            execution_id=f"exec-{cache_key}",
            remote=_remote(),
            state=cast(dict[str, Any], kickoff["state"]),
        )
        assert resumed["status"] == "running"

    result = _poll_until_terminal(runnable=runnable, argv_ptr=argv_ptr, cache_key=cache_key)
    assert result["status"] == expected_terminal
    if expected_terminal == "succeeded":
        assert result.get("error") is None


def test_script_executor_start_returns_running_with_job_state():
    script = "\n".join(["import time", "def fn(dag):", "    time.sleep(0.2)", "    return 1", ""])
    runnable = _mk_script_runnable(script)
    cache_key = "ck-start-handle"
    argv_ptr = _mk_argv_ptr(argv0=runnable)
    remote = _remote()

    executor = ScriptExecutor()
    result = executor.start(
        cache_key=cache_key,
        execution_id="exec-start-handle",
        runnable=runnable,
        argv_ptr=argv_ptr,
        remote=remote,
    )
    assert result["status"] == "running"

    job_state = cast(dict[str, Any], result["state"])
    assert isinstance(job_state.get("pid"), int)
    assert isinstance(job_state.get("result_path"), str)
    assert isinstance(job_state.get("stdout_path"), str)
    assert isinstance(job_state.get("stderr_path"), str)
    assert Path(cast(str, job_state["stdout_path"])).exists()
    assert Path(cast(str, job_state["stderr_path"])).exists()

    from daggerml.contrib.executors.script import _cleanup_workdir

    _cleanup_workdir(job_state)


def test_local_adapter_resolve_runnable_rejects_executor_for_other_adapter():
    class ForeignExecutor:
        name = "foreign"
        adapter = "lambda"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri("foreign"), adapter="x", kwargs={}, sub=None)

        @classmethod
        def handle(cls, *, cache_key, execution_id, state, runnable, argv_ptr, remote):
            return {"status": "running", "error": None, "state": {"token": execution_id}}

    ereg.register_executor(ForeignExecutor)
    with pytest.raises(DmlRepoError, match="is not registered for adapter 'local'"):
        LocalAdapter.resolve_runnable("foreign", {}, None)


def test_local_adapter_dispatches_to_executor_lifecycle():
    seen: dict[str, Any] = {}

    class DispatchExecutorForTest:
        name = "dispatch-test"
        adapter = "local"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @classmethod
        def handle(cls, *, cache_key, execution_id, state, runnable, argv_ptr, remote):
            seen["runnable"] = runnable
            seen["argv_ptr"] = argv_ptr
            seen["cache_key"] = cache_key
            seen["execution_id"] = execution_id
            seen["state"] = state
            seen["remote"] = remote
            return {"status": "succeeded", "error": None, "dag_id": "a" * 64}

    ereg.register_executor(DispatchExecutorForTest)
    runnable = Runnable(target=Uri("dispatch-test"), adapter="dml-local-adapter", kwargs={})
    payload = LocalAdapter.send(
        runnable=runnable,
        argv_ptr=_mk_argv_ptr("a"),
        cache_key="ck",
        execution_id="exec-ck",
        remote=_remote(),
        state=None,
    )
    payload = cast(dict[str, Any], payload)
    assert payload == {"status": "succeeded", "error": None, "dag_id": "a" * 64}
    assert isinstance(seen["argv_ptr"], str)
    assert seen["cache_key"] == "ck"


def test_local_adapter_unknown_executor_fails_deterministically():
    runnable = Runnable(target=Uri("missing"), adapter="dml-local-adapter", kwargs={})
    with pytest.raises(DmlRepoError, match="Executor 'missing' is not registered for adapter 'local'"):
        LocalAdapter.send(
            runnable=runnable,
            argv_ptr=_mk_argv_ptr(),
            cache_key="ck",
            execution_id="exec-ck",
            remote=_remote(),
            state=None,
        )


@pytest.mark.parametrize(
    "contract_id,executor_name,executor_cls,argv_args,expected",
    [
        pytest.param(
            "LRT-ADP-001",
            "echo",
            None,
            (1,),
            {"status": "succeeded", "error": None, "dag_id": "a" * 64},
            id="LRT-ADP-001:kickoff-terminal-succeeded-passthrough",
        ),
        pytest.param(
            "LRT-ADP-002",
            "running",
            "running",
            (),
            {"status": "running", "error": None, "state": {"token": "exec-ck"}},
            id="LRT-ADP-002:kickoff-running-passthrough",
        ),
    ],
)
def test_local_adapter_send_stage_matrix_LRT_ADP_001_to_LRT_ADP_002(
    contract_id, executor_name, executor_cls, argv_args, expected
):
    del contract_id

    if executor_cls == "running":
        class RunningExecutor:
            name = "running"
            adapter = "local"

            @staticmethod
            def resolve_runnable(uri, kwargs, sub):
                return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

            @classmethod
            def handle(cls, *, cache_key, execution_id, state, runnable, argv_ptr, remote):
                return {"status": "running", "error": None, "state": {"token": execution_id}}

        ereg.register_executor(RunningExecutor)

    runnable = Runnable(target=Uri(executor_name), adapter="dml-local-adapter", kwargs={})
    result = LocalAdapter.send(
        runnable=runnable,
        argv_ptr=_mk_argv_ptr(*argv_args),
        cache_key="ck",
        execution_id="exec-ck",
        remote=_remote(),
        state=None,
    )
    result = cast(dict[str, Any], result)
    assert result == expected


def test_local_adapter_send_rejects_non_contract_payload():
    class BadExecutor:
        name = "bad"
        adapter = "local"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @classmethod
        def handle(cls, *, cache_key, execution_id, state, runnable, argv_ptr, remote):
            return {"status": "running", "error": None, "state": {"token": execution_id}, "extra": 1}

    ereg.register_executor(BadExecutor)
    runnable = Runnable(target=Uri("bad"), adapter="dml-local-adapter", kwargs={})
    with pytest.raises(DmlRepoError, match="Adapter output"):
        LocalAdapter.send(
            runnable=runnable,
            argv_ptr=_mk_argv_ptr(),
            cache_key="ck",
            execution_id="exec-ck",
            remote=_remote(),
            state=None,
        )


def test_adapter_base_cli_reads_stdin_and_writes_stdout(capsys):
    class DummyAdapter(AdapterBase):
        @classmethod
        def send(cls, *, runnable, argv_ptr, cache_key, execution_id, remote, state):
            return {"status": "succeeded", "error": None, "dag_id": "a" * 64}

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "sys.stdin.read",
            lambda: DummyAdapter._dump_payload(
                runnable=Runnable(target=Uri("x"), adapter="dummy", kwargs={}),
                argv_ptr="ptr",
                cache_key="ck",
                execution_id="exec-ck",
                remote=_remote(),
                state=None,
            ).decode("utf-8"),
        )
        exit_code = DummyAdapter.cli([])

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out.strip()) == {"status": "succeeded", "error": None, "dag_id": "a" * 64}


def test_adapter_base_cli_reads_and_writes_files(tmp_path):
    class DummyAdapter(AdapterBase):
        @classmethod
        def send(cls, *, runnable, argv_ptr, cache_key, execution_id, remote, state):
            return {"status": "succeeded", "error": None, "dag_id": "a" * 64}

    in_file = tmp_path / "in.json"
    out_file = tmp_path / "out.json"
    in_file.write_bytes(
        DummyAdapter._dump_payload(
            runnable=Runnable(target=Uri("x"), adapter="dummy", kwargs={}),
            argv_ptr="ptr",
            cache_key="ck",
            execution_id="exec-ck",
            remote=_remote(),
            state=None,
        )
    )

    exit_code = DummyAdapter.cli(["-i", str(in_file), "-o", str(out_file)])
    assert exit_code == 0
    assert json.loads(out_file.read_text()) == {"status": "succeeded", "error": None, "dag_id": "a" * 64}
