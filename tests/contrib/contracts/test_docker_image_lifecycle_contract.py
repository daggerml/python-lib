"""Archive IDs, rather than tag aliases or Docker's load output, identify runs."""

import hashlib
import io
import json
import tarfile
import threading
from types import SimpleNamespace

import pytest

from daggerml.api import DmlRepoError
from daggerml.contrib.executors import docker as docker_module
from daggerml.contrib.executors._docker_archive import image_id_from_archive
from daggerml.contrib.executors.docker import DockerExecutor

pytestmark = pytest.mark.serial
CONFIG = b'{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]}}'
IMAGE_ID = "sha256:" + hashlib.sha256(CONFIG).hexdigest()


def image_archive(*, mode="w", oci=False, multiple=False):
    config_path = "config-not-a-digest.json"
    entries = {config_path: CONFIG, "manifest.json": json.dumps([
        {"RepoTags": ["worker:v1", "worker:latest"], "Config": config_path, "Layers": []}
    ] * (2 if multiple else 1)).encode()}
    if oci:
        manifest = json.dumps({"schemaVersion": 2, "config": {"digest": IMAGE_ID}, "layers": []}).encode()
        digest = hashlib.sha256(manifest).hexdigest()
        entries.update({
            "oci-layout": b'{"imageLayoutVersion":"1.0.0"}',
            "index.json": json.dumps({"manifests": [{"digest": "sha256:" + digest}]
                                      + ([{"digest": "sha256:" + "0" * 64}] if multiple else [])}).encode(),
            "blobs/sha256/" + digest: manifest,
            "blobs/sha256/" + IMAGE_ID.removeprefix("sha256:"): CONFIG,
        })
        # The OCI index takes precedence over compatibility metadata.
        entries[config_path] = b'{}'
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode=mode) as archive:
        for name, data in entries.items():
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    return output.getvalue()


def repack_archive(content, modify):
    with tarfile.open(fileobj=io.BytesIO(content), mode="r:*") as source:
        entries = {member.name: source.extractfile(member).read() for member in source}
    modify(entries)
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as target:
        for name, data in entries.items():
            member = tarfile.TarInfo(name)
            member.size = len(data)
            target.addfile(member, io.BytesIO(data))
    return output.getvalue()


@pytest.fixture
def docker(monkeypatch, tmp_path):
    backend = SimpleNamespace(archive=image_archive(), calls=[], fail_run=False, missing_image=False)

    def run(*args, **kwargs):
        backend.calls.append(args)
        if args[0] == "load":
            # Output deliberately has multiple tags and unrelated progress text.
            return "Progress...\nLoaded image: worker:v1\nLoaded image: worker:latest"
        if args[:2] == ("image", "inspect"):
            assert args[-1] == IMAGE_ID
            if backend.missing_image:
                raise DmlRepoError("No such image: " + IMAGE_ID)
            return json.dumps([{"Id": IMAGE_ID}])
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


@pytest.mark.parametrize("mode", ["w", "w:gz"])
@pytest.mark.parametrize("oci", [False, True])
def test_load_unchanged_archive_and_run_by_config_id(docker, tmp_path, mode, oci):
    docker.archive = image_archive(mode=mode, oci=oci)
    result = start()
    assert result["state"]["image"] == IMAGE_ID
    assert "cleanup_image" not in result["state"] and "archive_image" not in result["state"]
    launch = next(call for call in docker.calls if call[0] == "run")
    assert launch[launch.index("--label") + 1] == "daggerml.managed=true"
    assert IMAGE_ID in launch
    assert [call[0] for call in docker.calls] == ["load", "image", "run"]


@pytest.mark.parametrize("oci", [False, True])
def test_ambiguous_archive_is_rejected_before_loading(docker, oci):
    docker.archive = image_archive(oci=oci, multiple=True)
    with pytest.raises(DmlRepoError, match="exactly one image/platform"):
        start()
    assert docker.calls == []


def test_invalid_archive_is_rejected_before_loading(docker):
    docker.archive = b"not a tar"
    with pytest.raises(DmlRepoError, match="Cannot identify"):
        start()
    assert docker.calls == []


def test_load_must_make_expected_id_available(docker):
    docker.missing_image = True
    with pytest.raises(DmlRepoError, match="expected image ID"):
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


def test_missing_configuration_is_not_inferred_from_filename(tmp_path):
    path = tmp_path / "archive.tar"
    path.write_bytes(image_archive())
    assert image_id_from_archive(path) == IMAGE_ID


def test_oci_duplicate_tag_descriptors_are_one_image(tmp_path):
    def duplicate(entries):
        index = json.loads(entries["index.json"])
        index["manifests"] *= 2
        entries["index.json"] = json.dumps(index).encode()

    path = tmp_path / "archive.tar"
    path.write_bytes(repack_archive(image_archive(oci=True), duplicate))
    assert image_id_from_archive(path) == IMAGE_ID


def test_oci_nested_single_platform_index(tmp_path):
    def nest(entries):
        original = entries["index.json"]
        digest = hashlib.sha256(original).hexdigest()
        entries["blobs/sha256/" + digest] = original
        entries["index.json"] = json.dumps({"manifests": [{"digest": "sha256:" + digest}]}).encode()

    path = tmp_path / "archive.tar"
    path.write_bytes(repack_archive(image_archive(oci=True), nest))
    assert image_id_from_archive(path) == IMAGE_ID


def test_oci_config_digest_is_verified_before_load(docker):
    def corrupt(entries):
        entries["blobs/sha256/" + IMAGE_ID.removeprefix("sha256:")] = b'{}'

    docker.archive = repack_archive(image_archive(oci=True), corrupt)
    with pytest.raises(DmlRepoError, match="digest mismatch"):
        start()
    assert docker.calls == []
