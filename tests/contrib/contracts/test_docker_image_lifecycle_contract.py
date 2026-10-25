"""Docker owns archive interpretation; executor tags own image consumption."""

import io
import json
import tarfile
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from daggerml.api import DmlRepoError
from daggerml.contrib.executors import docker as docker_module
from daggerml.contrib.executors.docker import DockerExecutor, _cleanup_docker

pytestmark = pytest.mark.serial


def image_archive(tag, mode="w", oci=False):
    output = io.BytesIO()
    entries = {"manifest.json": [{"RepoTags": [tag], "Config": "config.json", "Layers": []}]}
    if oci:
        entries["oci-layout"] = {"imageLayoutVersion": "1.0.0"}
        entries["index.json"] = {"manifests": [{"annotations": {"io.containerd.image.name": tag}}]}
    with tarfile.open(fileobj=output, mode=mode) as archive:
        for name, payload in entries.items():
            data = json.dumps(payload).encode()
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    return output.getvalue()


class FakeDocker:
    def __init__(self):
        self.archive_tag = "daggerml-archive:build-123"
        self.archive = image_archive(self.archive_tag)
        self.image_id = "sha256:content"
        self.tags = {}
        self.calls = []
        self.block_load = None
        self.resume_load = None
        self.fail_run = False
        self.fail_inspection = False

    def run(self, *args, **kwargs):
        self.calls.append(args)
        if args[0] == "load":
            assert Path(args[-1]).read_bytes() == self.archive
            # Model modern Docker preferring OCI metadata when present.
            with tarfile.open(args[-1], "r:*") as archive:
                if "index.json" in archive.getnames():
                    tag = json.load(archive.extractfile("index.json"))["manifests"][0]["annotations"][
                        "io.containerd.image.name"]
                else:
                    tag = json.load(archive.extractfile("manifest.json"))[0]["RepoTags"][0]
            self.tags[tag] = self.image_id
            if self.block_load:
                self.block_load.set()
                assert self.resume_load.wait(timeout=5)
            return f"Loaded image: {tag}"
        if args[:2] == ("image", "inspect"):
            if self.fail_inspection:
                raise DmlRepoError("docker daemon unavailable")
            ref = args[-1]
            image_id = self.tags.get(ref)
            if image_id is None:
                raise DmlRepoError(f"No such image: {ref}")
            return json.dumps([dict(Id=image_id, RepoTags=sorted(
                tag for tag, value in self.tags.items() if value == image_id))])
        if args[:2] == ("image", "tag"):
            assert args[-2] == self.image_id
            self.tags[args[-1]] = args[-2]
            return ""
        if args[:2] == ("image", "rm"):
            assert "-f" not in args and "--force" not in args
            del self.tags[args[-1]]
            return ""
        if args[0] == "run":
            if self.fail_run:
                raise DmlRepoError("docker run failed")
            return "container"
        if args[0] == "inspect":
            return "exited"
        if args[0] == "rm":
            return ""
        pytest.fail(f"Unexpected Docker command: {args}")


@pytest.fixture
def docker(monkeypatch, tmp_path):
    backend = FakeDocker()
    monkeypatch.setattr(docker_module, "_IMAGE_LOCK_PATH", tmp_path / "locks" / "images.lock")
    monkeypatch.setattr(docker_module.shutil, "which", lambda _: "/docker")
    monkeypatch.setattr(DockerExecutor, "_run_docker", staticmethod(backend.run))
    monkeypatch.setattr(docker_module.S3Store, "from_remote_root", lambda _: SimpleNamespace(
        get=lambda _: backend.archive))
    monkeypatch.setattr(docker_module, "_write_scratch_json", lambda *args, **kwargs: None)
    return backend


def prepare(tmp_path, execution_id="exec-1"):
    workdir = tmp_path / execution_id
    workdir.mkdir()
    return DockerExecutor._prepare_image(
        {"kwargs": {"image": "s3://bucket/image.tar"}}, workdir, {"root": "s3://bucket/root"}, execution_id,
    )


@pytest.mark.parametrize("mode", ["w", "w:gz"], ids=["tar", "gzip"])
@pytest.mark.parametrize("oci", [False, True], ids=["legacy", "oci"])
def test_load_passes_archive_unchanged_and_tags_by_execution_id(docker, tmp_path, mode, oci):
    docker.archive = image_archive(docker.archive_tag, mode, oci)
    image, cleanup, archive = prepare(tmp_path)
    assert image == cleanup == "daggerml-execution:exec-1"
    assert archive == docker.archive_tag
    assert docker.tags == {archive: docker.image_id, image: docker.image_id}
    assert ("image", "tag", docker.image_id, image) in docker.calls
    _cleanup_docker("container", cleanup, "/docker", archive)
    assert docker.tags == {}
    _cleanup_docker("container", cleanup, "/docker", archive)
    assert docker.tags == {}


def test_archive_tag_is_retained_until_last_execution_finishes(docker, tmp_path):
    first, _, archive = prepare(tmp_path, "first")
    second, _, _ = prepare(tmp_path, "second")
    _cleanup_docker("container-first", first, "/docker", archive)
    assert docker.tags == {archive: docker.image_id, second: docker.image_id}
    _cleanup_docker("container-second", second, "/docker", archive)
    assert docker.tags == {}


def test_other_consumer_tags_keep_archive_tag(docker, tmp_path):
    image, _, archive = prepare(tmp_path)
    docker.tags["user:keep"] = docker.image_id
    _cleanup_docker("container", image, "/docker", archive)
    assert docker.tags == {archive: docker.image_id, "user:keep": docker.image_id}
    del docker.tags["user:keep"]
    _cleanup_docker("container", image, "/docker", archive)
    assert docker.tags == {}


def test_foreign_archive_tag_is_never_owned_or_removed(docker, tmp_path):
    docker.archive_tag = "daggerml-lifecycle:ci"
    docker.archive = image_archive(docker.archive_tag)
    image, _, archive = prepare(tmp_path)
    assert archive is None
    _cleanup_docker("container", image, "/docker", archive)
    assert docker.tags == {docker.archive_tag: docker.image_id}


def test_fully_qualified_archive_tag_uses_same_lifecycle(docker, tmp_path):
    docker.archive_tag = "docker.io/library/" + docker.archive_tag
    docker.archive = image_archive(docker.archive_tag, oci=True)
    image, _, archive = prepare(tmp_path)
    _cleanup_docker("container", image, "/docker", archive)
    assert docker.tags == {}


def test_daemon_failure_does_not_claim_successful_cleanup(docker, tmp_path):
    image, _, archive = prepare(tmp_path)
    docker.fail_inspection = True
    with pytest.raises(DmlRepoError, match="daemon unavailable"):
        _cleanup_docker("container", image, "/docker", archive)
    assert image in docker.tags and archive in docker.tags


def test_launch_failure_releases_execution_and_unconsumed_archive_tags(docker):
    docker.fail_run = True
    with pytest.raises(DmlRepoError, match="docker run failed"):
        DockerExecutor().start(
            cache_key="cache", execution_id="failed", runnable={"sub": {"adapter": "nested"},
            "kwargs": {"image": "s3://bucket/image.tar"}}, remote={"root": "s3://bucket/root"},
            scratch_uri="s3://bucket/root/exec/io/failed",
        )
    assert docker.tags == {}


def test_start_persists_archive_reference_for_fresh_process_cleanup(docker):
    result = DockerExecutor().start(
        cache_key="cache", execution_id="exec", runnable={"sub": {"adapter": "nested"},
        "kwargs": {"image": "s3://bucket/image.tar"}}, remote={"root": "s3://bucket/root"},
        scratch_uri="s3://bucket/root/exec/io/exec",
    )
    assert result["state"]["cleanup_image"] == "daggerml-execution:exec"
    assert result["state"]["archive_image"] == docker.archive_tag


@pytest.mark.parametrize("operation", ["cleanup", "cancel"])
def test_terminal_operations_release_owned_tags(docker, operation):
    executor = DockerExecutor()
    kwargs = dict(cache_key="cache", execution_id="exec", runnable={"sub": {"adapter": "nested"},
                  "kwargs": {"image": "s3://bucket/image.tar"}}, remote={"root": "s3://bucket/root"},
                  scratch_uri="s3://bucket/root/exec/io/exec")
    state = executor.start(**kwargs)["state"]
    if operation == "cleanup":
        result = executor.cleanup(**kwargs, state=state, result_ref="dag:result")
        assert result["status"] == "success"
    else:
        result = executor.cancel(**kwargs, state=state, cancel_requested_by="user")
        assert result["status"] == "cancelled"
    assert result["state"] == state
    assert docker.tags == {}


def test_load_and_cleanup_are_coordinated_across_threads(docker, tmp_path):
    first, _, archive = prepare(tmp_path, "first")
    docker.block_load, docker.resume_load = threading.Event(), threading.Event()
    cleaned = threading.Event()
    failures = []

    def load():
        try:
            prepare(tmp_path, "second")
        except BaseException as exc:
            failures.append(exc)

    def cleanup():
        try:
            _cleanup_docker("container-first", first, "/docker", archive)
        except BaseException as exc:
            failures.append(exc)
        finally:
            cleaned.set()

    loader, cleaner = threading.Thread(target=load), threading.Thread(target=cleanup)
    loader.start()
    try:
        assert docker.block_load.wait(timeout=5)
        cleaner.start()
        assert not cleaned.wait(timeout=0.05)
    finally:
        docker.resume_load.set()
        loader.join(timeout=5)
        if cleaner.ident is not None:
            cleaner.join(timeout=5)
    assert not loader.is_alive() and not cleaner.is_alive()
    assert not failures
    assert docker.tags == {archive: docker.image_id, "daggerml-execution:second": docker.image_id}
