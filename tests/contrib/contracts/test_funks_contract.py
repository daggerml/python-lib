"""Contracts for contrib funk helpers."""

import gzip
from types import SimpleNamespace

import pytest

from daggerml.api import DmlRepoError
from daggerml.contrib.executors.script import ScriptExecutor
from daggerml.contrib.funks import _gzip_file, _run, docker_build


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
    assert "_gzip_file(image_tar, compressed_image_tar)" in script
    assert "store.put(filepath=compressed_image_tar, suffix='.tar.gz')" in script
    assert script.index("return dag.put(Uri(remote_image), name='remote-image')") < script.index(
        "compressed_image_tar = './image.tar.gz'"
    )


def test_contrib_funks_003__gzip_file_writes_gzip_stream(tmp_path):
    source = tmp_path / "image.tar"
    destination = tmp_path / "image.tar.gz"
    source.write_bytes(b"docker-image-archive")

    _gzip_file(str(source), str(destination))

    assert gzip.decompress(destination.read_bytes()) == source.read_bytes()
