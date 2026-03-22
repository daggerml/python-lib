from __future__ import annotations

import json
import os
import time
from typing import Any, cast

import pytest

from daggerml import Dml
from daggerml._internal.types import Runnable, Uri
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib.adapters import LocalAdapter
from daggerml.contrib.executor_state import LocalState
from daggerml.contrib.executors import ScriptExecutor
from daggerml.contrib.s3 import S3Store


@pytest.fixture(autouse=True)
def _reset_registry(monkeypatch, tmp_path):
    ereg._reset_for_tests()
    monkeypatch.setenv("DML_FN_CACHE_DIR", str(tmp_path / "state-default"))

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
    ereg._reset_for_tests()


def _remote() -> dict[str, str]:
    return {"root": os.environ["DML_REMOTE_ROOT"], "cache": "test-cache"}


def _mk_argv_ptr(*args: Any, argv0: Any | None = None) -> str:
    with Dml.temporary() as dml:
        dag = dml.new("argv-src", "argv-src")
        index_ref = dag._require_index_ref()
        head = argv0 if argv0 is not None else Runnable(target=Uri("daggerml:list"), kwargs={}, adapter="")
        fn_ref = dml.index.put_literal(index_ref, head)
        arg_refs = [dml.index.put_literal(index_ref, value) for value in args]
        with dml.index._tx(readonly=False) as txn:
            argv_ref = dml.index._prepare_fn(index_ref, [fn_ref, *arg_refs], {}, txn)
        return dml.index._remote_ops().put_ptr(argv_ref)


def _mk_script_runnable(script: str) -> Runnable:
    uri = S3Store().put(data=script.encode("utf-8"), suffix=".py")
    return Runnable(
        target=Uri("script"),
        adapter="dml-local-adapter",
        kwargs={"__dml_script_exec__": {"prepop": {}, "fn_name": "fn", "script_uri": uri.uri}},
    )


def test_local_adapter_uses_comms_local_root_dir(tmp_path):
    cache_key = "ck-comms-root"
    root_dir = tmp_path / "state-custom"
    in_file = tmp_path / "input.json"
    out_file = tmp_path / "output.json"
    in_file.write_bytes(
        LocalAdapter._dump_payload(
            runnable=Runnable(target=Uri("echo"), kwargs={}, adapter="dml-local-adapter"),
            argv_ptr=_mk_argv_ptr(),
            cache_key=cache_key,
            remote=_remote(),
            comms={"kind": "local", "spec": {"cache_dir": str(root_dir)}},
        )
    )
    assert LocalAdapter.cli(["-i", str(in_file), "-o", str(out_file)]) == 0
    result = json.loads(out_file.read_text())
    assert result == {"status": "succeeded", "error": None}
    assert (root_dir / f"{cache_key}.json").exists()


def test_script_executor_state_owner_and_wrapper_fields_preserved():
    runnable = _mk_script_runnable("\n".join(["def fn(dag):", "    return 1", ""]))
    cache_key = "ck-owner"
    argv_ptr = _mk_argv_ptr(argv0=runnable)

    first = LocalAdapter.send(runnable=runnable, argv_ptr=argv_ptr, cache_key=cache_key, remote=_remote())
    assert first == {"status": "running", "error": None}
    record = LocalState(cache_key).get()
    assert isinstance(record, dict)
    assert record.get("version") == 1
    assert record.get("cache_key") == cache_key
    assert isinstance(record.get("heartbeat_ts"), float)

    with ScriptExecutor.state_class(cache_key).lock() as state:
        assert state is not None
        updated = state.set_executor_metadata(executor_id="wrapper", data={"tag": "x"})
        state.update(updated)

    for _ in range(200):
        result = LocalAdapter.send(runnable=runnable, argv_ptr=argv_ptr, cache_key=cache_key, remote=_remote())
        if result["status"] in {"succeeded", "failed"}:
            break
        time.sleep(0.01)
    else:
        pytest.fail("script executor did not reach terminal state")

    final = LocalState(cache_key).get()
    assert final is None
