from typing import cast

import pytest

import daggerml as dml
from daggerml.api import Dml
from tests import temporary_dml


@pytest.fixture(autouse=True)
def _reset_default_runtime():
    dml.clear_default_dml()
    yield
    dml.clear_default_dml()


def _default_info(status: dict) -> dict:
    return status["default"]


def _repo_status(status: dict) -> dict:
    return status["status"]


def test_default_runtime_status_DRT_STS_001_reports_implicit_default_creation_source():
    dml.clear_default_dml()
    status = dml.status()
    info = _default_info(status)
    assert info["source"] == "implicit"
    assert info["has_scoped_override"] is False
    assert info["has_process_default"] is True

    repo = _repo_status(status)
    assert set(repo.keys()) == {"head", "branches", "dags", "indexes"}
    assert repo["head"] is None
    assert repo["branches"] == []
    assert repo["dags"] == {}
    assert repo["indexes"] == []


def test_default_runtime_status_DRT_STS_002_get_default_dml_is_cached_process_default():
    dml.clear_default_dml()
    dml0 = dml.get_default_dml()
    dml1 = dml.get_default_dml()
    assert dml0 is dml1

    status = dml.status()
    assert _default_info(status)["source"] == "process"


def test_default_runtime_status_DRT_STS_003_set_and_scoped_default_runtime_resolution():
    with temporary_dml(repo="a") as raw_a, temporary_dml(repo="b") as raw_b:
        dml_a = cast(Dml, raw_a)
        dml_b = cast(Dml, raw_b)
        dml.set_default_dml(dml_a)
        assert dml.get_default_dml() is dml_a
        assert _default_info(dml.status())["source"] == "process"

        with dml.use_default_dml(dml_b):
            assert dml.get_default_dml() is dml_b
            scoped = _default_info(dml.status())
            assert scoped["source"] == "scoped"
            assert scoped["has_scoped_override"] is True

        assert dml.get_default_dml() is dml_a
        assert _default_info(dml.status())["source"] == "process"


def test_default_runtime_status_DRT_STS_004_top_level_new_and_load_delegate_to_default_runtime():
    with temporary_dml(repo="default-runtime") as raw_dml:
        default_dml = cast(Dml, raw_dml)
        dml.set_default_dml(default_dml)
        with dml.new(dml=default_dml, name="d0", message="msg") as dag:
            dag.put(42, name="n0")
            dag.commit("ok")

        loaded = dml.load("d0", dml=default_dml)
        assert loaded.result.value() == "ok"
        assert loaded["n0"].value() == 42


def test_temporary_runtime_uses_default_branch_for_active_head():
    with temporary_dml(repo="temp-head") as raw_runtime:
        runtime = cast(Dml, raw_runtime)
        assert runtime._context.project_home is not None
        assert runtime.branch()["head"] == "main"
