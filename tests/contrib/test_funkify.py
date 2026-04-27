from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, cast

import pytest

from daggerml import Dml, clear_default_dml, set_default_dml
from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib import adapter_registry as areg
from daggerml.contrib import api
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib.adapters import LocalAdapter
from daggerml.contrib.executors import ScriptExecutor
from daggerml.contrib.executors.script import run_payload
from daggerml.contrib.testing import defunkify


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
        def handle(cls, *, cache_key, execution_id, state, runnable, argv_ptr, remote):
            return {"status": "running", "error": None, "state": {"token": execution_id}}

    class CustomExecutor:
        name = "custom"
        adapter = "local"

        @staticmethod
        def resolve_runnable(uri, kwargs, sub):
            return Runnable(target=Uri(uri), kwargs=dict(kwargs), sub=sub, adapter="dml-local-adapter")

        @classmethod
        def handle(cls, *, cache_key, execution_id, state, runnable, argv_ptr, remote):
            return {"status": "running", "error": None, "state": {"token": execution_id}}

    ereg.register_executor(ScriptExecutor)
    ereg.register_executor(InnerExecutor)
    ereg.register_executor(CustomExecutor)
    yield
    areg._reset_for_tests()
    ereg._reset_for_tests()


def _remote() -> dict[str, str]:
    return {"root": os.environ["DML_REMOTE_URI"]}


def _mk_argv_ptr(*args: Any, argv0: Any | None = None) -> str:
    with Dml.temporary() as dml:
        dag = dml.new("argv-src", "argv-src")
        index_ref = dag._require_index_ref()
        head = argv0 if argv0 is not None else Runnable(target=Uri("daggerml:list"), kwargs={}, adapter="")
        fn_ref = dml.index.put_literal(index_ref, head)
        arg_refs = [dml.index.put_literal(index_ref, value) for value in args]
        with dml.index._tx(readonly=False) as txn:
            argv_ref = dml.index._prepare_fn(index_ref, [fn_ref, *arg_refs], {}, txn)
        return dml.index._remote_ops().put_ref_manifest(argv_ref)


def _poll_until_terminal(
    *, runnable: Runnable, argv_ptr: str, cache_key: str, initial_state: dict[str, Any] | None = None
) -> dict[str, Any]:
    execution_id = f"exec-{cache_key}"
    state: dict[str, Any] | None = initial_state
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
    with Dml.temporary() as dml:
        src = dml.new("src", "src")
        src.commit(9)

        dag = dml.new("dst", "dst")
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


def test_funkify_script_runnable_contains_executable_fn_script():
    def helper(x):
        return x + 1

    def fn(dag, x, y=2):
        return helper(x) + y

    with Dml.temporary() as dml:
        dag = dml.new("dst", "dst")
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


def test_funkify_script_integration_runs_to_completion_with_decorator():
    decorate = api.funkify(uri="script", adapter="local")

    @decorate
    def fn(dag, x, y=2, *, z=3):
        print("running-script")
        return x.value() + y.value() + z.value()  # pyright: ignore[reportAttributeAccessIssue]

    with Dml.temporary() as dml:
        dag = dml.new("dst-int", "dst-int")
        runnable = cast(Runnable, dag.put(cast(Any, fn)).value())
        argv_ptr = _mk_argv_ptr(4, argv0=runnable)
        cache_key = "ck-funkify-int-1"

        kickoff = LocalAdapter.send(
            runnable=runnable,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
            execution_id=f"exec-{cache_key}",
            remote=_remote(),
            state=None,
        )
        assert kickoff["status"] == "running"

        result = _poll_until_terminal(
            runnable=runnable,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
            initial_state=cast(dict[str, Any], kickoff["state"]),
        )
        assert result["status"] == "succeeded", result


def test_funkify_script_integration_runs_with_prepop_from_subchain_using_decorator():
    decorate = api.funkify(uri="script", adapter="local")

    @decorate
    def fn(dag):
        return dag.seed.value() * 2

    with Dml.temporary() as dml:
        dag = dml.new("dst-prepop", "dst-prepop")
        runnable = cast(Runnable, dag.put(cast(Any, fn)).value())

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
        cache_key = "ck-funkify-int-2"

        result = _poll_until_terminal(
            runnable=runnable,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
        )
        assert result["status"] == "succeeded", result


def test_dagclass_compiled_method_executes_through_local_script_runtime():
    @api.dagclass
    class Example:
        x: Any = 2

        def main(self, a, b=1):
            return self.x.value() + a.value() * b.value()  # pyright: ignore[reportAttributeAccessIssue]

    with Dml.temporary() as dml:
        set_default_dml(dml)
        try:
            result = api.run(Example(), 4, b=3, name="dagclass-runtime-int")
            assert result is None

            loaded = dml.load("dagclass-runtime-int")
            assert loaded["x"].value() == 2
            assert loaded["<dagclass-call>"].value() == 14
            assert loaded.result.value() == 14
        finally:
            clear_default_dml()


def test_funkify_script_runtime_executes_generated_source_with_args_and_kwargs(tmp_path):
    def fn(dag, x, y=2, *, z=3):
        return x.value() + y.value() + z.value()  # pyright: ignore[reportAttributeAccessIssue]

    delayed = api.funkify(fn, uri="script", adapter="local")
    with Dml.temporary() as dml:
        dag = dml.new("dst-worker", "dst-worker")
        runnable = cast(Runnable, dag.put(cast(Any, delayed)).value())
        os.environ["DML_PROJECT_HOME"] = cast(str, dml.repo)
        try:
            result = run_payload(_mk_argv_ptr(4, argv0=runnable))
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
        def send(*, runnable, argv_ptr, cache_key, execution_id, remote, state):
            return {"status": "running", "error": None, "state": {"token": execution_id}}

        @staticmethod
        def cli(argv=None):
            return 0

    areg.register_adapter(BadAdapter())

    with Dml.temporary() as dml:
        dag = dml.new("d0", "d0")
        delayed = api.funkify(lambda dag: None, uri="script", adapter="bad")
        with pytest.raises(DmlRepoError, match="resolve_runnable must return Runnable"):
            dag.put(cast(Any, delayed))
