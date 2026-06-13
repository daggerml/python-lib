from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import patch


def _load_example_module(name: str, relative_path: str):
    repo_root = Path(__file__).resolve().parents[3]
    module_path = repo_root / relative_path
    spec = importlib.util.spec_from_file_location(name, module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_contrib_examples_001__moto_helper_binds_for_docker_and_publishes_loopback_endpoint():
    helper = _load_example_module("moto_server_env", "examples/moto_server_env.py")

    with patch.object(helper.platform, "system", return_value="Linux"):
        bind_host, endpoint = helper._server_binding(39209)

    assert bind_host == "0.0.0.0"
    assert endpoint == "http://127.0.0.1:39209"


def test_contrib_examples_002__moto_helper_uses_loopback_binding_on_macos():
    helper = _load_example_module("moto_server_env", "examples/moto_server_env.py")

    with patch.object(helper.platform, "system", return_value="Darwin"):
        bind_host, endpoint = helper._server_binding(39209)

    assert bind_host == "127.0.0.1"
    assert endpoint == "http://127.0.0.1:39209"
