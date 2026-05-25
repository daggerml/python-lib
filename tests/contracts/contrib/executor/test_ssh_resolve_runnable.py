from __future__ import annotations

import pytest

from daggerml._internal.types import DmlRepoError, Runnable, Uri
from daggerml.contrib import adapter_registry as areg
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib.adapters import LocalAdapter
from daggerml.contrib.executors import ScriptExecutor, SshExecutor


@pytest.fixture(autouse=True)
def _reset_registries(tmp_path, monkeypatch):
    areg._reset_for_tests()
    ereg._reset_for_tests()
    monkeypatch.setenv("DML_TEST_FN_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("DML_REMOTE_ROOT", "s3://test-bucket/test-prefix")
    areg.register_adapter(LocalAdapter)
    ereg.register_executor(ScriptExecutor)
    ereg.register_executor(SshExecutor)
    yield
    areg._reset_for_tests()
    ereg._reset_for_tests()


def _sub_runnable() -> Runnable:
    return Runnable(target=Uri("script"), adapter="dml-local-adapter", kwargs={"x": 1}, sub=None)


def test_local_adapter_resolve_runnable_SSH_RES_001_returns_expected_ssh_runnable_shape():
    sub = _sub_runnable()
    result = LocalAdapter.resolve_runnable(
        "ssh",
        {"host": "worker.example", "flags": ["-p", "2222"], "env_files": ["/etc/dml.env"]},
        sub,
    )

    assert isinstance(result, Runnable)
    assert result.target.uri == "ssh"
    assert result.adapter == "dml-local-adapter"
    assert result.sub is sub
    assert result.kwargs == {
        "host": "worker.example",
        "flags": ["-p", "2222"],
        "env_files": ["/etc/dml.env"],
    }
@pytest.mark.parametrize(
    "kwargs,sub,expected_error",
    [
        pytest.param({"host": "worker.example"}, None, "requires sub runnable", id="SSH-RES-002:missing-sub-runnable"),
        pytest.param({}, _sub_runnable(), "requires non-empty host", id="SSH-RES-002:missing-host"),
        pytest.param(
            {"host": "worker.example", "user": "alice"},
            _sub_runnable(),
            "Unknown ssh executor kwargs",
            id="SSH-RES-002:rejects-unknown-kwargs",
        ),
    ],
)
def test_local_adapter_resolve_runnable_SSH_RES_002_rejects_invalid_inputs(kwargs, sub, expected_error):
    with pytest.raises(DmlRepoError, match=expected_error):
        LocalAdapter.resolve_runnable("ssh", kwargs, sub)
