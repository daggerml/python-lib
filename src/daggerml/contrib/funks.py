from __future__ import annotations

from daggerml.contrib import api


def _run(*cmd: str) -> None:
    import subprocess

    from daggerml._internal.types import DmlRepoError

    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if proc.returncode == 0:
        return
    raise DmlRepoError(
        f"Command failed ({proc.returncode}): {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )


@api.funkify(uri="script", adapter="local", extra_objs=(_run,))
def docker_build(dag, context_tarball, build_flags=(), repo=None):
    from uuid import uuid4

    from daggerml import Uri
    from daggerml.contrib.s3 import S3Store

    build_flags = tuple(build_flags.value())

    store = S3Store()
    tag = uuid4().hex
    local_image = f"dml:{tag}"
    store.untar(context_tarball.value(), ".")
    _run("docker", "build", *build_flags, "-t", local_image, ".")
    repo = repo.value() if repo is not None else None
    if repo is not None:
        remote_image = f"{repo.uri}:{tag}"
        _run("docker", "tag", local_image, remote_image)
        _run("docker", "push", remote_image)
        return dag.put(Uri(remote_image), name="remote-image")
    image_tar = "./image.tar"
    _run("docker", "save", "-o", str(image_tar), local_image)
    return store.put(filepath=str(image_tar), suffix=".tar")


__all__ = ["docker_build"]
