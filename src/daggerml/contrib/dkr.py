#!/usr/bin/env python3
from daggerml.contrib import funkify


def dkr_build(tarball_uri, repo_uri, build_flags=(), session=None):
    import subprocess
    from shutil import which
    from tempfile import NamedTemporaryFile, TemporaryDirectory
    from urllib.parse import urlparse
    from uuid import uuid4

    import boto3

    session = session or boto3
    p = urlparse(tarball_uri)
    docker = which("docker")
    assert docker is not None
    with TemporaryDirectory() as tmpd:
        with NamedTemporaryFile(suffix=".tar") as tmpf:
            session.client("s3").download_file(p.netloc, p.path[1:], tmpf.name)
            subprocess.run(["tar", "-xvf", tmpf.name, "-C", tmpd], check=True)
        _tag = uuid4().hex
        image_tag = f"{repo_uri}:{_tag}"
        resp = subprocess.run(
            [docker, "build", *build_flags, "-t", image_tag, tmpd],
            check=False,
            stderr=subprocess.PIPE,
        )
        if resp.returncode != 0:
            raise RuntimeError(f"docker build failed:\n{resp.stderr.decode()}")
    return _tag


def dkr_push(image_tag, repo_uri, session=None):
    import re
    import subprocess
    from shutil import which

    import boto3

    docker = which("docker")
    client = (session or boto3).client("ecr")
    auth_token = client.get_authorization_token()["authorizationData"][0]["authorizationToken"]
    resp = subprocess.run(
        [docker, "login", "--username", "AWS", "--password-stdin", repo_uri],
        input=auth_token,
        check=False,
        text=True,
        stderr=subprocess.PIPE,
    )
    if resp.returncode != 0:
        raise RuntimeError(f"docker login failed:\n{resp.stderr}")
    subprocess.run([docker, "push", f"{repo_uri}:{image_tag}"], check=True)
    repo_name = re.match(r".*\.dkr\.ecr\.[^.]+\.amazonaws\.com/(.*)$", repo_uri).group()
    response = client.describe_images(repositoryName=repo_name, imageIds=[{"imageTag": image_tag}])
    image_digest = response["imageDetails"][0]["imageDigest"]
    return image_digest


@funkify(extra_fns=[dkr_build])
def build_dag(dag):
    from daggerml import Resource

    tarball = dag.argv[1].value()
    repo = dag.argv[2].value()
    flags = dag.argv[3].value() if len(dag.argv) > 3 else []
    dag.tag = dkr_build(tarball.uri, repo.uri, flags)
    dag.uri = f"{repo.uri}:{dag.tag.value()}"
    dag.result = Resource(dag.uri.value())


@funkify(extra_fns=[dkr_build, dkr_push])
def build_n_push_dag(dag):
    from daggerml import Resource

    tarball = dag.argv[1].value()
    repo = dag.argv[2].value()
    flags = dag.argv[3].value() if len(dag.argv) > 3 else []
    dag.tag = dkr_build(tarball.uri, repo.uri, flags)
    dag.digest = dkr_push(dag.tag, repo.uri)
    dag.uri = f"{repo.uri}:{dag.tag.value()}@{dag.digest.value()}"
    dag.result = Resource(dag.uri.value())
