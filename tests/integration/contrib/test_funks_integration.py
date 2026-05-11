from __future__ import annotations

import io
import json
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

from daggerml import Dml, Uri, new
from daggerml.contrib import api
from daggerml.contrib.funks import docker_build
from daggerml.contrib.s3 import S3Store
from daggerml.contrib.testing import MockNode, defunkify

pytestmark = pytest.mark.slow


class FakeDag:
    def put(self, value, *, name=None):
        assert isinstance(value, Uri)
        return MockNode(value)


ASSETS_DIR = Path(__file__).parents[2] / "contrib" / "assets" / "docker_build_ctx"


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    probe = subprocess.run(["docker", "info"], check=False, capture_output=True, text=True)
    return probe.returncode == 0


def _saved_image_tag(image_tarball: bytes) -> str:
    with tarfile.open(fileobj=io.BytesIO(image_tarball), mode="r") as tf:
        manifest = json.loads(tf.extractfile("manifest.json").read())
    return manifest[0]["RepoTags"][0]


def test_docker_build_is_funkified_script_callable():
    assert isinstance(docker_build, api.DelayedRunnable)
    assert docker_build.uri == "script"
    assert docker_build.adapter == "local"
    assert defunkify(docker_build).__name__ == "docker_build"


def test_docker_build_builds_and_uploads_image_tar(monkeypatch):
    calls: list[tuple[str, ...]] = []
    call = defunkify(docker_build)

    class FakeStore:
        def untar(self, tar_uri, dest, *, unsafe=False):
            assert tar_uri == Uri("s3://bucket/context.tar")
            assert unsafe is False

        def put(self, data=None, filepath=None, *, suffix=""):
            assert data is None
            assert filepath is not None
            assert suffix == ".tar"
            return Uri("s3://bucket/image.tar")

    class FakeUuid:
        hex = "abc123"

    monkeypatch.setattr("uuid.uuid4", lambda: FakeUuid())
    monkeypatch.setattr("daggerml.contrib.s3.S3Store", FakeStore)
    monkeypatch.setattr("daggerml.contrib.funks._run", lambda *cmd: calls.append(cmd))

    result = call(FakeDag(), Uri("s3://bucket/context.tar"), ["--platform=linux/amd64", "--no-cache"])

    assert result == Uri("s3://bucket/image.tar")
    assert calls[0][:4] == ("docker", "build", "--platform=linux/amd64", "--no-cache")
    assert calls[0][-2:] == ("dml:abc123", calls[0][-1])
    assert calls[1][:3] == ("docker", "save", "-o")
    assert calls[1][-1] == "dml:abc123"


def test_docker_build_pushes_when_repo_is_provided(monkeypatch):
    calls: list[tuple[str, ...]] = []
    call = defunkify(docker_build)

    class FakeStore:
        def untar(self, tar_uri, dest, *, unsafe=False):
            return None

        def put(self, data=None, filepath=None, *, suffix=""):
            return Uri("s3://bucket/image.tar")

    class FakeUuid:
        hex = "abc123"

    monkeypatch.setattr("uuid.uuid4", lambda: FakeUuid())
    monkeypatch.setattr("daggerml.contrib.s3.S3Store", FakeStore)
    monkeypatch.setattr("daggerml.contrib.funks._run", lambda *cmd: calls.append(cmd))

    result = call(FakeDag(), Uri("s3://bucket/context.tar"), [], Uri("repo/name"))

    assert result == MockNode(Uri("repo/name:abc123"))
    assert ("docker", "tag", "dml:abc123", "repo/name:abc123") in calls
    assert ("docker", "push", "repo/name:abc123") in calls


def test_docker_build_in_dag_builds_runnable_image(tmp_path):
    if not _docker_available():
        pytest.skip("docker daemon is not available")

    store = S3Store()
    context_tarball = store.tar(ASSETS_DIR)
    call = defunkify(docker_build)

    with Dml.temporary() as dml:
        with new(dml=dml, name="docker-build-int", message="docker-build-int") as dag:
            image_tar_uri = call(dag, context_tarball)

    assert isinstance(image_tar_uri, Uri)
    image_tarball = store.get(image_tar_uri)
    image_tag = _saved_image_tag(image_tarball)

    subprocess.run(["docker", "image", "rm", "-f", image_tag], check=False, capture_output=True, text=True)

    tmp_tar = tmp_path / "docker-image.tar"
    try:
        tmp_tar.write_bytes(image_tarball)
        load = subprocess.run(["docker", "load", "-i", str(tmp_tar)], check=True, capture_output=True, text=True)
        assert image_tag in (load.stdout + load.stderr)

        run = subprocess.run(["docker", "run", "--rm", image_tag], check=True, capture_output=True, text=True)
        assert run.stdout.strip() == "docker-build-ok"
    finally:
        tmp_tar.unlink(missing_ok=True)
        subprocess.run(["docker", "image", "rm", "-f", image_tag], check=False, capture_output=True, text=True)
