from __future__ import annotations

from daggerml.contrib import api


def _run(*cmd: str) -> None:
    import subprocess

    from daggerml.api import DmlRepoError

    proc = subprocess.run(cmd, check=False)
    if proc.returncode == 0:
        return
    raise DmlRepoError(
        f"Command failed (exit code {proc.returncode}): {' '.join(cmd)}. See the execution logs for command output."
    )


def _gzip_file(source: str, destination: str) -> None:
    import gzip
    import shutil

    with open(source, "rb") as src, gzip.open(destination, "wb") as dst:
        shutil.copyfileobj(src, dst)


@api.funkify(uri="script", adapter="local", extra_objs=(_run, _gzip_file))
def docker_build(dag, context_tarball, build_flags=(), repo=None):
    from contextlib import chdir
    from tempfile import TemporaryDirectory
    from uuid import uuid4

    from daggerml import Uri
    from daggerml.contrib.s3 import S3Store

    build_flags = tuple(build_flags.value())

    store = S3Store()
    tag = uuid4().hex
    local_image = f"dml:{tag}"
    with TemporaryDirectory(prefix="dml-docker-build-") as build_dir:
        store.untar(context_tarball.value(), build_dir)
        with chdir(build_dir):
            _run("docker", "build", *build_flags, "-t", local_image, ".")
            repo = repo.value() if repo is not None else None
            if repo is not None:
                remote_image = f"{repo.uri}:{tag}"
                _run("docker", "tag", local_image, remote_image)
                _run("docker", "push", remote_image)
                return dag.put(Uri(remote_image), name="remote-image")
            image_tar = "./image.tar"
            _run("docker", "save", "-o", str(image_tar), local_image)
            compressed_image_tar = "./image.tar.gz"
            _gzip_file(image_tar, compressed_image_tar)
            return store.put(filepath=compressed_image_tar, suffix=".tar.gz")


__all__ = ["docker_build"]
