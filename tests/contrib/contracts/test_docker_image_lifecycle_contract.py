"""Docker, not archive metadata, determines the image identity used for runs."""

import hashlib
import json
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from daggerml.api import DmlRepoError
from daggerml.contrib.executors import docker as docker_module
from daggerml.contrib.executors.docker import DockerExecutor

pytestmark = pytest.mark.serial
CONFIG = b'{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]}}'
CONFIG_ID = "sha256:" + hashlib.sha256(CONFIG).hexdigest()
IMAGE_ID = "sha256:" + "a" * 64


@pytest.fixture
def docker(monkeypatch, tmp_path):
    backend = SimpleNamespace(
        # Intentionally opaque: archive interpretation belongs entirely to Docker.
        archive=b"Docker-managed archive bytes", calls=[], fail_run=False, missing_image=False,
        output="Progress...\nLoaded image: worker:v1\nLoaded image: worker:latest",
        refs={"worker:v1": IMAGE_ID, "worker:latest": IMAGE_ID, CONFIG_ID: IMAGE_ID},
    )

    def run(*args, **kwargs):
        backend.calls.append(args)
        if args[0] == "load":
            assert Path(args[-1]).read_bytes() == backend.archive
            return backend.output
        if args[:2] == ("image", "inspect"):
            if backend.missing_image:
                raise DmlRepoError("No such image: " + args[-1])
            return json.dumps([{"Id": backend.refs[args[-1]]}])
        if args[0] == "run":
            if backend.fail_run:
                raise DmlRepoError("docker run failed")
            return "container"
        if args[0] == "inspect":
            return "exited"
        if args[0] == "stop":
            return "container"
        pytest.fail(f"Unexpected Docker command: {args}")

    monkeypatch.setattr(docker_module, "_IMAGE_LOCK_PATH", tmp_path / "locks" / "images.lock")
    monkeypatch.setattr(docker_module.shutil, "which", lambda _: "/docker")
    monkeypatch.setattr(DockerExecutor, "_run_docker", staticmethod(run))
    monkeypatch.setattr(docker_module.S3Store, "from_remote_root", lambda _: SimpleNamespace(
        get=lambda _: backend.archive))
    monkeypatch.setattr(docker_module, "_write_scratch_json", lambda *args, **kwargs: None)
    return backend


def start():
    return DockerExecutor().start(
        cache_key="cache", execution_id="exec", runnable={"sub": {"adapter": "nested"},
        "kwargs": {"image": "s3://bucket/image.tar"}}, remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/root/exec/io/exec",
    )


def test_load_unchanged_archive_and_run_by_docker_id(docker):
    result = start()
    assert result["state"]["image"] == IMAGE_ID
    assert "cleanup_image" not in result["state"] and "archive_image" not in result["state"]
    launch = next(call for call in docker.calls if call[0] == "run")
    assert launch[launch.index("--label") + 1] == "daggerml.managed=true"
    assert IMAGE_ID in launch
    assert [call[0] for call in docker.calls] == ["load", "image", "image", "run"]
    assert {call[-1] for call in docker.calls if call[:2] == ("image", "inspect")} == {
        "worker:v1", "worker:latest",
    }


def test_distinct_loaded_image_ids_are_rejected_after_loading(docker):
    docker.refs["worker:v1"] = "sha256:" + "b" * 64
    with pytest.raises(DmlRepoError, match="exactly one image ID"):
        start()
    assert docker.calls[0][0] == "load"
    assert not any(call[0] == "run" or "rm" in call for call in docker.calls)


@pytest.mark.parametrize("output", ["", "Progress only", "Loaded image: ", "Loaded image ID: "])
def test_load_must_report_nonempty_image_references(docker, output):
    docker.output = output
    with pytest.raises(DmlRepoError, match="did not report any image references"):
        start()
    assert len(docker.calls) == 1 and docker.calls[0][0] == "load"


def test_load_must_make_reported_references_available(docker):
    docker.missing_image = True
    with pytest.raises(DmlRepoError, match="unavailable image"):
        start()
    assert not any(call[0] == "run" for call in docker.calls)


def test_launch_failure_keeps_loaded_images(docker):
    docker.fail_run = True
    with pytest.raises(DmlRepoError, match="docker run failed"):
        start()
    assert not any("rm" in call or "tag" in call for call in docker.calls)


@pytest.mark.parametrize("operation", ["cleanup", "cancel"])
def test_terminal_operations_retain_resources(docker, operation):
    state = start()["state"]
    kwargs = dict(cache_key="cache", execution_id="exec", runnable={}, state=state,
                  remote={}, scratch_uri="s3://bucket/root/exec/io/exec")
    if operation == "cleanup":
        result = DockerExecutor().cleanup(**kwargs, result_ref="dag:result")
        assert result["status"] == "success"
    else:
        result = DockerExecutor().cancel(**kwargs, cancel_requested_by="user")
        assert result["status"] == "cancelled"
        assert ("stop", "container") in docker.calls
    assert not any("rm" in call for call in docker.calls)


def test_lock_is_held_through_container_creation(docker, monkeypatch):
    original = DockerExecutor._run_docker
    blocked = threading.Event()
    acquired = threading.Event()
    waiter = None

    def acquire():
        blocked.set()
        with docker_module._image_lock():
            acquired.set()

    def run(*args, **kwargs):
        nonlocal waiter
        if args[0] == "run":
            waiter = threading.Thread(target=acquire)
            waiter.start()
            assert blocked.wait(5)
            assert not acquired.wait(0.05)
        return original(*args, **kwargs)

    monkeypatch.setattr(DockerExecutor, "_run_docker", staticmethod(run))
    try:
        start()
    finally:
        if waiter is not None:
            waiter.join(5)
    assert acquired.is_set()


@pytest.mark.parametrize("reported_id", [CONFIG_ID, IMAGE_ID])
def test_untagged_load_uses_inspected_id_not_reported_digest(docker, reported_id):
    docker.output = f"Loaded image ID: {reported_id}"
    docker.refs[reported_id] = IMAGE_ID
    assert start()["state"]["image"] == IMAGE_ID
    assert ("image", "inspect", reported_id) in docker.calls


def test_classic_docker_config_id_is_also_supported(docker):
    docker.refs = {ref: CONFIG_ID for ref in docker.refs}
    assert start()["state"]["image"] == CONFIG_ID


def test_duplicate_reported_references_are_inspected_once(docker):
    docker.output = "Loaded image: worker:v1\nLoaded image: worker:v1"
    assert start()["state"]["image"] == IMAGE_ID
    assert docker.calls.count(("image", "inspect", "worker:v1")) == 1


def test_load_failure_is_propagated_without_launch_or_cleanup(docker, monkeypatch):
    def fail(*args, **kwargs):
        assert args[0] == "load"
        raise DmlRepoError("docker rejected the archive")

    monkeypatch.setattr(DockerExecutor, "_run_docker", staticmethod(fail))
    with pytest.raises(DmlRepoError, match="rejected the archive"):
        start()
