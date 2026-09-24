"""Docker-capable end-to-end worker scenarios with real Moto transport."""

import json
import os
import shutil
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from urllib.parse import urlparse

import pytest

import daggerml.api as api
from daggerml import Runnable, Uri
from daggerml.contrib import api as contrib_api
from daggerml.contrib.api import funkify
from daggerml.contrib.codecs import DelayedActionCodec
from daggerml.contrib.s3 import S3Store

pytestmark = [pytest.mark.slow, pytest.mark.docker, pytest.mark.serial]


@funkify(uri="script")
def docker_value(dag, number):
    return number.value() * 2


@funkify(uri="script")
def docker_failure(dag, number):
    return number.value() / 0


@contrib_api.dagclass
class ContainerCalculation:
    image: object
    flags: object
    factor: int

    @funkify(uri="docker", image=contrib_api.ref("image"), flags=contrib_api.ref("flags"))
    @funkify(uri="script")
    def main(self, number):
        return number.value() * self.factor.value()


@pytest.fixture
def docker_world(runtime_world, monkeypatch):
    image = os.getenv("LIFECYCLE_DOCKER_IMAGE")
    if not image or not shutil.which("docker"):
        pytest.skip("Docker and LIFECYCLE_DOCKER_IMAGE are required")
    if subprocess.run(["docker", "info"], capture_output=True, timeout=10).returncode:
        pytest.skip("Docker daemon is not available")
    monkeypatch.setattr(
        api, "_codecs",
        [(1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()), (0, 3, api.MiscPyTypeCodec())],
    )
    endpoint = urlparse(os.environ["AWS_ENDPOINT_URL"])
    flags = ["--add-host=host.docker.internal:host-gateway", "-e",
             f"AWS_ENDPOINT_URL=http://host.docker.internal:{endpoint.port}"]
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "AWS_DEFAULT_REGION"):
        flags.extend(["-e", f"{key}={os.environ[key]}"])
    return runtime_world, image, flags


@pytest.mark.parametrize("tar_image", [False, True], ids=["local-image", "s3-image-tar"])
def test_docker_poll_result_and_image_loading(docker_world, tmp_path, monkeypatch, tar_image):
    world, image, flags = docker_world
    if tar_image:
        archive = tmp_path / "image.tar"
        subprocess.run(["docker", "save", "-o", str(archive), image], check=True, timeout=120)
        with api.new("image-seed", dml=world.publisher) as seed:
            seed.commit(seed.put("image transport"))
        monkeypatch.setenv("DML_REMOTE_ROOT", world.root)
        image = S3Store().put(filepath=archive, suffix=".tar").uri
    wrapped = funkify(docker_value, uri="docker", image=image, flags=flags)
    with api.new("docker-result", dml=world.publisher) as dag:
        dag.commit(dag.put(wrapped)(21, timeout=120_000))
    assert api.load("docker-result", dml=world.publisher).result.value() == 42


def test_docker_reports_worker_failure(docker_world):
    world, image, flags = docker_world
    wrapped = funkify(docker_failure, uri="docker", image=image, flags=flags)
    with api.new("docker-failure", dml=world.publisher) as dag:
        with pytest.raises(api.Error):
            dag.put(wrapped)(21, timeout=120_000, name="failed")
        dag.commit(dag.put("retained"))
    with pytest.raises(api.NodeError, match="division by zero"):
        _ = api.load("docker-failure", dml=world.publisher)["failed"]


def test_nested_dagclass_runs_in_docker(docker_world):
    world, image, flags = docker_world
    with api.use_default_dml(world.publisher):
        contrib_api.run(ContainerCalculation(image=image, flags=flags, factor=2), 21, name="docker-dagclass")
    assert api.load("docker-dagclass", dml=world.publisher).result.value() == 42


def test_docker_cleanup_retry_and_cancel_cross_adapter_processes(docker_world):
    world, image, _flags = docker_world
    executable = str(Path(sys.executable).with_name("dml-local-adapter"))
    runnable = asdict(Runnable(
        target=Uri("docker"), kwargs={"image": image, "flags": []},
        sub=Runnable(target=Uri("script"), kwargs={}, adapter="dml-local-adapter"),
        adapter="dml-local-adapter",
    ))

    def send(operation, container_id):
        request = {
            "operation": operation, "cache_key": "docker-lifecycle", "execution_id": container_id,
            "runnable": runnable, "remote": {"root": world.root},
            "scratch_uri": f"{world.root}/exec/io/{container_id}/",
            "adapter_state": {"container_id": container_id},
        }
        if operation == "cleanup":
            request["result_ref"] = "dag:result"
        else:
            request.update(argv_ref="node-argv:docker-lifecycle", requested_by="tester")
        proc = subprocess.run([executable, "-i", "-", "-o", "-"], input=json.dumps(request),
                              capture_output=True, text=True, timeout=30)
        assert proc.returncode == 0, proc.stderr
        return json.loads(proc.stdout)

    def start_container():
        return subprocess.run(["docker", "run", "-d", image, "python", "-c", "import time;time.sleep(30)"],
                              check=True, capture_output=True, text=True, timeout=15).stdout.strip()

    cleanup_id = start_container()
    cancel_id = start_container()
    try:
        assert send("cleanup", cleanup_id)["status"] == "retry"
        subprocess.run(["docker", "stop", "-t", "1", cleanup_id], check=True, capture_output=True, timeout=10)
        assert send("cleanup", cleanup_id)["status"] == "success"
        assert send("cancel", cancel_id)["status"] == "cancelled"
        for container_id in (cleanup_id, cancel_id):
            inspection = subprocess.run(["docker", "inspect", container_id], capture_output=True, timeout=10)
            assert inspection.returncode != 0
    finally:
        for container_id in (cleanup_id, cancel_id):
            subprocess.run(["docker", "rm", "-f", container_id], capture_output=True, timeout=10)
