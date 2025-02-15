#!/usr/bin/env python3
import base64
import re
import subprocess
from shutil import which
from tempfile import NamedTemporaryFile, TemporaryDirectory
from urllib.parse import urlparse
from uuid import uuid4

import boto3

from daggerml import Resource


def _run_cmd(cmd, input=None):
    resp = subprocess.run(cmd, check=False, text=True, stderr=subprocess.PIPE, input=input)
    if resp.returncode != 0:
        msg = f"command: {cmd} failed\nSTDERR:\n-------\n{resp.stderr}"
        raise RuntimeError(msg)


def dkr_build(tarball_uri, build_flags=(), session=None):
    session = session or boto3
    p = urlparse(tarball_uri)
    docker = which("docker")
    assert docker is not None
    with TemporaryDirectory() as tmpd:
        with NamedTemporaryFile(suffix=".tar") as tmpf:
            session.client("s3").download_file(p.netloc, p.path[1:], tmpf.name)
            subprocess.run(["tar", "-xvf", tmpf.name, "-C", tmpd], check=True)
        _tag = uuid4().hex
        image_tag = f"dml:{_tag}"
        resp = subprocess.run(
            [docker, "build", *build_flags, "-t", image_tag, tmpd],
            check=False,
            stderr=subprocess.PIPE,
        )
        if resp.returncode != 0:
            raise RuntimeError(f"docker build failed:\n{resp.stderr.decode()}")
    return {"image": Resource(image_tag), "tag": _tag}


def dkr_login(client):
    auth_response = client.get_authorization_token()
    auth_data = auth_response["authorizationData"][0]
    auth_token = auth_data["authorizationToken"]
    proxy_endpoint = auth_data["proxyEndpoint"]
    decoded_token = base64.b64decode(auth_token).decode("utf-8")
    username, password = decoded_token.split(":")
    login_cmd = ["docker", "login", "-u", username, "--password-stdin", proxy_endpoint]
    _run_cmd(login_cmd, input=password)


def dkr_push(local_image, repo_uri):
    client = boto3.client("ecr")
    dkr_login(client)
    tag = local_image.uri.split(":")[-1]
    remote_image = f"{repo_uri}:{tag}"
    _run_cmd(["docker", "tag", local_image.uri, remote_image])
    _run_cmd(["docker", "push", remote_image])
    (repo_name,) = re.match(r"^[^/]+/([^:]+)$", repo_uri).groups()
    response = client.describe_images(repositoryName=repo_name, imageIds=[{"imageTag": tag}])
    digest = response["imageDetails"][0]["imageDigest"]
    return {
        "image": Resource(f"{repo_uri}:{tag}@{digest}"),
        "tag": tag,
        "digest": digest,
    }
