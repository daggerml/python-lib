import pytest

import daggerml as dml


@pytest.fixture(autouse=True)
def _reset_default_runtime():
    dml.clear_default_dml()
    yield
    dml.clear_default_dml()


def _default_info(status: dict) -> dict:
    return status["default"]


def _runtime_info(status: dict) -> dict:
    return status["runtime"]


def _config_info(status: dict) -> dict:
    return status["config"]


def test_status_implicit_default_creation():
    dml.clear_default_dml()
    status = dml.status()
    info = _default_info(status)
    assert info["source"] == "implicit"
    assert info["has_scoped_override"] is False
    assert info["has_process_default"] is True

    cfg = _config_info(status)
    assert set(cfg.keys()) == {"repo", "branch", "user", "config_dir", "remote"}
    assert set(cfg["remote"].keys()) == {"root", "cache"}

    runtime = _runtime_info(status)
    assert runtime["ops_initialized"] is False
    assert isinstance(runtime["head_ref"], str)


def test_get_default_dml_is_cached_process_default():
    dml.clear_default_dml()
    dml0 = dml.get_default_dml()
    dml1 = dml.get_default_dml()
    assert dml0 is dml1

    status = dml.status()
    assert _default_info(status)["source"] == "process"


def test_set_and_scoped_default_runtime_resolution():
    with dml.Dml.temporary(repo="a") as dml_a, dml.Dml.temporary(repo="b") as dml_b:
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


def test_top_level_new_and_load_delegate_to_default_runtime():
    with dml.Dml.temporary(repo="default-runtime") as default_dml:
        dml.set_default_dml(default_dml)
        with dml.new("d0", "msg") as dag:
            dag.put(42, name="n0")
            dag.commit("ok")

        loaded = dml.load("d0")
        assert loaded.result.value() == "ok"
        assert loaded["n0"].value() == 42
