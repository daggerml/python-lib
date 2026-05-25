from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, cast

import pytest

from daggerml import clear_default_dml, load, new, set_default_dml
from daggerml._internal.dml import make_index_ops, with_db
from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.codecs import CodecError
from daggerml.contrib import adapter_registry as areg
from daggerml.contrib import api
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib.adapters import LocalAdapter
from daggerml.contrib.executors import ScriptExecutor
from daggerml.contrib.executors.script import run_payload
from daggerml.contrib.testing import defunkify
from tests import temporary_dml

pytestmark = pytest.mark.slow


@pytest.fixture(autouse=True)
def _reset_registry(tmp_path, monkeypatch):
    areg._reset_for_tests()
    ereg._reset_for_tests()
    monkeypatch.setenv("DML_TEST_FN_STATE_DIR", str(tmp_path / "state"))
    areg.register_adapter(LocalAdapter)

    class InnerExecutor:
        name = "inner"
        adapter = "local"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @classmethod
        def handle(
            cls,
            *,
            cache_key,
            execution_id,
            state,
            execution_status,
            cancel_requested_by,
            runnable,
            argv_ptr,
            remote,
        ):
            return {"status": "running", "error": None, "state": {"token": execution_id}}

    class CustomExecutor:
        name = "custom"
        adapter = "local"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @classmethod
        def handle(
            cls,
            *,
            cache_key,
            execution_id,
            state,
            execution_status,
            cancel_requested_by,
            runnable,
            argv_ptr,
            remote,
        ):
            return {"status": "running", "error": None, "state": {"token": execution_id}}

    ereg.register_executor(ScriptExecutor)
    ereg.register_executor(InnerExecutor)
    ereg.register_executor(CustomExecutor)
    yield
    areg._reset_for_tests()
    ereg._reset_for_tests()


def _remote() -> dict[str, str]:
    return {"root": os.environ["DML_REMOTE_ROOT"]}


def _mk_argv_ptr(*args: Any, argv0: Any | None = None) -> str:
    with temporary_dml() as dml:
        dag = new(dml=dml, name="argv-src", message="argv-src")
        index_ref = dag._require_index_ref()
        head = argv0 if argv0 is not None else Runnable(target=Uri("daggerml:list"), kwargs={}, adapter="")
        with with_db(dml) as db:
            index_ops = make_index_ops(db, dml)
            fn_ref = index_ops.put_literal(index_ref, head)
            arg_refs = [index_ops.put_literal(index_ref, value) for value in args]
            with index_ops._tx(readonly=False) as txn:
                argv_ref = index_ops._prepare_fn(index_ref, [fn_ref, *arg_refs], {}, txn)
            return index_ops._remote_ops().put_ref_manifest(argv_ref)


def _poll_until_terminal(
    *, runnable: Runnable, argv_ptr: str, cache_key: str, initial_state: dict[str, Any] | None = None
) -> dict[str, Any]:
    execution_id = f"exec-{cache_key}"
    state: dict[str, Any] | None = initial_state
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        result = LocalAdapter.send(
            runnable=runnable,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
            execution_id=execution_id,
            remote=_remote(),
            state=state,
            execution_status=None,
            cancel_requested_by=None,
        )
        if state is None and result.get("status") == "running":
            state = cast(dict[str, Any], result.get("state"))
        if result["status"] in {"succeeded", "failed"}:
            return cast(dict[str, Any], result)
        time.sleep(0.01)
    pytest.fail("script executor did not reach terminal state within 5.0s")


def test_funkify_decorator_returns_delayed_runnable():
    def fn_impl(dag, x=1):
        return x

    fn = api.funkify(fn_impl, uri="script", adapter="local")

    assert isinstance(fn, api.DelayedRunnable)
    assert fn.uri == "script"
    assert fn.adapter == "local"
    assert fn.sub is None
    assert "fn" in fn.kwargs
    assert defunkify(fn).__wrapped__ is fn_impl


def test_funkify_wrapper_returns_delayed_runnable():
    inner = api.DelayedRunnable(uri="inner", adapter="local", sub=None, kwargs={})
    wrapped = api.funkify(inner, uri="script", adapter="local", x=1)
    assert isinstance(wrapped, api.DelayedRunnable)
    assert wrapped.sub is inner
    assert wrapped.kwargs == {"x": 1}


def test_funkify_wrapper_preserves_innermost_script_for_defunkify():
    def fn_impl(dag):
        return 1

    inner = api.funkify(fn_impl, uri="script", adapter="local")

    wrapped = api.funkify(inner, uri="custom", adapter="local", x=1)

    assert defunkify(wrapped).__wrapped__ is fn_impl


def test_funkify_invalid_input_fails():
    with pytest.raises(DmlRepoError, match="Invalid funkify input"):
        api.funkify(cast(Any, 123), uri="script", adapter="local")


def test_funkify_with_ref_and_load_normalizes_via_codec():
    with temporary_dml() as dml:
        src = new(dml=dml, name="src", message="src")
        src.commit(9)

        dag = new(dml=dml, name="dst", message="dst")
        dag.a = 7
        inner = api.DelayedRunnable(uri="inner", adapter="local", sub=None, kwargs={})
        delayed = api.funkify(inner, uri="custom", adapter="local", x=api.ref("a"), y=api.load("src"))
        node = dag.put(cast(Any, delayed))
        rv = node.value()
        assert isinstance(rv, Runnable)
        assert rv.target.uri == "custom"
        assert rv.adapter == "dml-local-adapter"
        assert rv.kwargs["x"] == 7
        assert rv.kwargs["y"] == 9


def test_funkify_resolve_runnable_receives_python_values_for_delayed_refs_and_loads():
    seen: dict[str, Any] = {}

    class CaptureExecutor:
        name = "capture"
        adapter = "local"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            seen["uri"] = uri
            seen["kwargs"] = dict(kwargs)
            seen["sub"] = sub
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @classmethod
        def handle(
            cls,
            *,
            cache_key,
            execution_id,
            state,
            execution_status,
            cancel_requested_by,
            runnable,
            argv_ptr,
            remote,
        ):
            del cls, cache_key, execution_id, state, execution_status, cancel_requested_by, runnable, argv_ptr, remote
            return {"status": "running", "error": None, "state": {"token": "capture"}}

    ereg.register_executor(CaptureExecutor)

    with temporary_dml() as dml:
        src = new(dml=dml, name="src", message="src")
        src.answer = 9
        src.commit(11)

        dag = new(dml=dml, name="dst", message="dst")
        dag.host = "worker.example"
        dag.flags = ["-p", "2222"]
        inner = api.DelayedRunnable(uri="inner", adapter="local", sub=None, kwargs={})
        delayed = api.funkify(
            inner,
            uri="capture",
            adapter="local",
            host=api.ref("host"),
            flags=api.ref("flags"),
            answer=api.load("src", "answer"),
        )

        rv = dag.put(cast(Any, delayed)).value()

    assert isinstance(rv, Runnable)
    assert seen["uri"] == "capture"
    assert seen["kwargs"] == {
        "host": "worker.example",
        "flags": ["-p", "2222"],
        "answer": 9,
    }


@pytest.mark.parametrize("resolved_adapter", ["podman-adapter", "/opt/acme/bin/acme-adapter"])
def test_funkify_plugin_adapter_sugar_resolves_to_concrete_runtime_adapter(resolved_adapter):
    @dataclass
    class PluginAdapter:
        name: str = "gpu"
        executable: str = resolved_adapter

        def resolve_runnable(self, uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter=self.executable)

        @staticmethod
        def send(runnable, argv_ptr, cache_key, execution_id, remote, state, execution_status, cancel_requested_by):
            return {"status": "running", "error": None, "state": {"token": execution_id}}

        @staticmethod
        def cli(argv=None):
            return 0

    areg.register_adapter(PluginAdapter())

    with temporary_dml() as dml:
        dag = new(dml=dml, name="gpu", message="gpu")
        inner = api.DelayedRunnable(uri="inner", adapter="local", sub=None, kwargs={})
        delayed = api.funkify(inner, uri="custom", adapter="gpu")
        rv = dag.put(cast(Any, delayed)).value()

    assert isinstance(rv, Runnable)
    assert rv.target.uri == "custom"
    assert rv.adapter == resolved_adapter


def test_funkify_script_runnable_contains_executable_fn_script():
    def helper(x):
        return x + 1

    def fn(dag, x, y=2):
        return helper(x) + y

    with temporary_dml() as dml:
        dag = new(dml=dml, name="dst", message="dst")
        delayed = api.funkify(fn, uri="script", adapter="local", extra_objs=[helper])
        node = dag.put(cast(Any, delayed))
        rv = node.value()
        assert isinstance(rv, Runnable)
        assert "script" not in rv.kwargs
        assert isinstance(rv.kwargs.get("__dml_script_exec__"), dict)
        from daggerml.contrib.s3 import S3Store

        script = S3Store().get(rv.kwargs["__dml_script_exec__"]["script_uri"]).decode("utf-8")

    namespace: dict[str, Any] = {}
    exec(script, namespace)
    result = namespace["fn"](object(), 4, y=3)
    assert result == 8


@pytest.mark.parametrize(
    "contract_id,stage,use_prepop,cache_key",
    [
        pytest.param(
            "FKY-LFC-001", "kickoff", False, "ck-funkify-int-1-kickoff", id="FKY-LFC-001:kickoff-returns-running"
        ),
        pytest.param(
            "FKY-LFC-002", "resume", False, "ck-funkify-int-1-resume", id="FKY-LFC-002:resume-poll-returns-running"
        ),
        pytest.param(
            "FKY-LFC-003", "terminal", False, "ck-funkify-int-1-terminal", id="FKY-LFC-003:terminal-succeeded"
        ),
        pytest.param(
            "FKY-LFC-004",
            "terminal",
            True,
            "ck-funkify-int-2",
            id="FKY-LFC-004:terminal-succeeded-with-subchain-prepop",
        ),
    ],
)
def test_funkify_script_lifecycle_stage_matrix_FKY_LFC_001_to_FKY_LFC_004(contract_id, stage, use_prepop, cache_key):
    del contract_id
    decorate = api.funkify(uri="script", adapter="local")
    nonce = {"kickoff": 1, "resume": 2, "terminal": 3}[stage]

    if use_prepop:

        @decorate
        def fn(dag):
            return dag.seed.value() * 2
    else:

        @decorate
        def fn(dag, x, y=2, *, z=3, nonce=nonce):
            return x.value() + y.value() + z.value()  # pyright: ignore[reportAttributeAccessIssue]

    with temporary_dml() as dml:
        dag_name = "dst-prepop" if use_prepop else "dst-int"
        dag = new(dml=dml, name=dag_name, message=dag_name)
        runnable = cast(Runnable, dag.put(cast(Any, fn)).value())

        if use_prepop:
            meta = cast(dict[str, Any], runnable.kwargs["__dml_script_exec__"])
            inner = Runnable(
                target=Uri("inner"),
                adapter="dml-local-adapter",
                kwargs={
                    "__dml_script_exec__": {
                        "prepop": {"seed": 6},
                        "fn_name": cast(str, meta["fn_name"]),
                        "script_uri": cast(str, meta["script_uri"]),
                    }
                },
                sub=None,
            )
            outer = Runnable(target=Uri("outer"), adapter="dml-local-adapter", kwargs={}, sub=inner)
            argv_ptr = _mk_argv_ptr(argv0=outer)
        else:
            argv_ptr = _mk_argv_ptr(4, argv0=runnable)

        kickoff = LocalAdapter.send(
            runnable=runnable,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
            execution_id=f"exec-{cache_key}",
            remote=_remote(),
            state=None,
            execution_status=None,
            cancel_requested_by=None,
        )
        assert kickoff["status"] == "running"

        if stage == "kickoff":
            return

        resumed = LocalAdapter.send(
            runnable=runnable,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
            execution_id=f"exec-{cache_key}",
            remote=_remote(),
            state=cast(dict[str, Any], kickoff["state"]),
            execution_status=None,
            cancel_requested_by=None,
        )
        assert resumed["status"] == "running"
        if stage == "resume":
            return

        result = _poll_until_terminal(
            runnable=runnable,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
            initial_state=cast(dict[str, Any], resumed["state"]),
        )
        assert result["status"] == "succeeded", result


def test_dagclass_compiled_method_executes_through_local_script_runtime():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self, a, b=1):
            return self.x.value() + a.value() * b.value()  # pyright: ignore[reportAttributeAccessIssue]

    with temporary_dml() as dml:
        set_default_dml(dml)
        try:
            result = api.run(Example(), 4, b=3, name="dagclass-runtime-int")
            assert result is None

            loaded = load("dagclass-runtime-int", dml=dml)
            assert loaded["x"].value() == 2
            assert loaded["<dagclass-call>"].value() == 14
            assert loaded.result.value() == 14
        finally:
            clear_default_dml()


def test_funkify_script_runtime_executes_generated_source_with_args_and_kwargs(tmp_path):
    nonce = len(str(tmp_path))

    def fn(dag, x, y=2, *, z=3, nonce=nonce):
        return x.value() + y.value() + z.value()  # pyright: ignore[reportAttributeAccessIssue]

    delayed = api.funkify(fn, uri="script", adapter="local")
    with temporary_dml() as dml:
        dag = new(dml=dml, name="dst-worker", message="dst-worker")
        runnable = cast(Runnable, dag.put(cast(Any, delayed)).value())
        os.environ["DML_PROJECT_HOME"] = cast(str, dml._context.project_home)
        try:
            result = run_payload(
                _mk_argv_ptr(4, argv0=runnable),
                execution_id="exec-worker",
                cache_key="ck-worker",
                remote_root=_remote()["root"],
            )
        finally:
            os.environ.pop("DML_PROJECT_HOME", None)

    assert result["status"] == "succeeded"
    assert result["error"] is None
    assert isinstance(result["dag_id"], str)
    assert result["dag_id"]


def test_funkify_resolve_runnable_requires_runnable_return():
    @dataclass
    class BadAdapter:
        name: str = "bad"
        executable: str = "bad-adapter"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return (uri, kwargs, sub)

        @staticmethod
        def send(runnable, argv_ptr, cache_key, execution_id, remote, state, execution_status, cancel_requested_by):
            return {"status": "running", "error": None, "state": {"token": execution_id}}

        @staticmethod
        def cli(argv=None):
            return 0

    areg.register_adapter(BadAdapter())

    with temporary_dml() as dml:
        dag = new(dml=dml, name="d0", message="d0")
        delayed = api.funkify(lambda dag: None, uri="script", adapter="bad")
        with pytest.raises(CodecError, match="resolve_runnable must return Runnable"):
            dag.put(cast(Any, delayed))
