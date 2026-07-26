"""Contracts for contrib funk helpers."""

from types import SimpleNamespace

import pytest

from daggerml.api import DmlRepoError
from daggerml.contrib.executors.script import ScriptExecutor
from daggerml.contrib.funks import _run, docker_build


def test_contrib_funks_001__run_streams_output_and_reports_exit_code(monkeypatch):
    calls = []

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=17)

    monkeypatch.setattr("subprocess.run", fake_run)

    with pytest.raises(DmlRepoError, match="exit code 17.*execution logs"):
        _run("docker", "build", ".")

    assert calls == [((("docker", "build", "."),), {"check": False})]


def test_contrib_funks_002__docker_build_isolated_script_context():
    _, script = ScriptExecutor._script_kwargs(docker_build.kwargs)

    assert "from contextlib import chdir" in script
    assert "with TemporaryDirectory(prefix='dml-docker-build-') as build_dir:" in script
