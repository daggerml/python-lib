"""Composable real-boundary worlds for slow lifecycle tests.

Only the Moto service is session shared; each world owns independent repository
homes and an isolated S3 prefix. No runtime operations are replaced by fakes.
"""

from __future__ import annotations

import subprocess
import sys
import venv
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pytest

import daggerml.api as api
from daggerml import Dml


@dataclass
class RemoteWorld:
    root: str
    publisher: Dml
    home: Path

    def clone(self, name: str, revision: str = "main", *, depth: int | None = None) -> Dml:
        return Dml.clone(revision, project_home=str(self.home / name), remote_root=self.root, user=name, depth=depth)


@pytest.fixture
def local_project(tmp_path, monkeypatch, remote_env, s3_bucket):
    del remote_env, s3_bucket
    monkeypatch.setenv("DML_DEFAULT_DB_MAP_SIZE_MAX", str(64 * 1024 * 1024))

    def create(name="project"):
        home = tmp_path / name
        home.mkdir()
        return Dml.init(str(home), user=name, remote_root=f"s3://test-bucket/test-prefix/local-{uuid4().hex}")

    return create


@pytest.fixture
def remote_world(tmp_path, monkeypatch, remote_env, s3_bucket):
    del remote_env, s3_bucket
    monkeypatch.setenv("DML_DEFAULT_DB_MAP_SIZE_MAX", str(64 * 1024 * 1024))

    def create(name="world"):
        home = tmp_path / name
        home.mkdir()
        root = f"s3://test-bucket/test-prefix/lifecycle-{uuid4().hex}"
        publisher_home = home / "publisher"
        publisher_home.mkdir()
        publisher = Dml.init(str(publisher_home), user="publisher", remote_root=root)
        return RemoteWorld(root, publisher, home)

    return create


@pytest.fixture
def collaboration_world(remote_world):
    def create(name="collaboration"):
        world = remote_world(name)
        with api.new("seed", dml=world.publisher) as dag:
            dag.commit(dag.put(1, name="value"))
        world.publisher.push()
        return world

    return create


@pytest.fixture
def satellite_world(remote_world):
    def create():
        primary = remote_world("primary")
        satellite = remote_world("satellite")
        with api.new("satellite-input", dml=satellite.publisher) as dag:
            dag.commit(dag.put({"answer": 42}, name="data"))
        satellite.publisher.push()
        return primary, satellite

    return create


@pytest.fixture
def shallow_merge_world(collaboration_world):
    def create():
        world = collaboration_world("merge-history")
        producer = world.publisher
        producer.branch.create("side")
        with api.new("main-input", dml=producer) as dag:
            dag.commit(dag.put("main"))
        producer.checkout("side")
        with api.new("side-input", dml=producer) as dag:
            dag.commit(dag.put("side"))
        producer.checkout("main")
        producer.merge("side", ff_only=False)
        producer.push()
        return world

    return create


@pytest.fixture
def runtime_world(remote_world):
    """Remote project for production cache/script tests (no patched index ops)."""
    return remote_world("runtime")


@pytest.fixture(scope="session")
def installed_wheel(tmp_path_factory):
    """Build once and install a non-editable wheel in an isolated virtualenv."""
    root = Path(__file__).resolve().parents[1]
    output = tmp_path_factory.mktemp("installed-dml")
    subprocess.run(["uv", "build", "--wheel", "--out-dir", str(output)], cwd=root, check=True, timeout=300)
    wheel = next(output.glob("daggerml-*.whl"))
    env = output / "venv"
    venv.create(env, with_pip=True)
    python = env / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    subprocess.run([str(python), "-m", "pip", "install", f"{wheel}[dashboard,terminal]"], check=True, timeout=300)
    return env
