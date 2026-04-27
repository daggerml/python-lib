"""Run an end-to-end Docker-backed dataset pipeline.

The example builds a Docker image from this repository, starts a local moto S3
server when no remote URI is configured, loads the iris dataset in one
Docker-executed funk, and trains a small classifier in another. It exercises
the contrib runtime end to end: script funkification, Docker execution, remote
cache publication, and S3-backed artifact exchange between DAG nodes.
"""

from __future__ import annotations

import os
from pathlib import Path
from time import time
from typing import Any
from urllib.parse import urlparse

import boto3
import polars as pl

from daggerml import Dml
from daggerml.contrib import api
from daggerml.contrib.funks import docker_build
from daggerml.contrib.s3 import S3Store

EXCLUDE_PATTERNS = (
    # ".git",  # we need .git to install lib from the repo
    ".venv/*",
    ".mypy_cache/*",
    ".pytest_cache/*",
    "__pycache__/*",
    "*.pyc",
    "tests/*",
)
REPO_ROOT = Path(__file__).resolve().parents[1]


def _start_local_moto_if_needed() -> Any | None:
    if os.environ.get("DML_REMOTE_URI"):
        return None
    try:
        for key in list(os.environ.keys()):
            if key.startswith("AWS_"):
                del os.environ[key]
        os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
        from moto.server import ThreadedMotoServer
    except ModuleNotFoundError as e:
        raise RuntimeError("Set DML_REMOTE_URI for a real S3 bucket, or install moto[server] for local dev.") from e
    server = ThreadedMotoServer(port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    os.environ.setdefault("AWS_REGION", "us-east-1")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ["AWS_ENDPOINT_URL"] = endpoint
    os.environ["DML_REMOTE_URI"] = "s3://daggerml-example/artifacts"
    boto3.client("s3", endpoint_url=endpoint).create_bucket(Bucket="daggerml-example")
    return server


@api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("dkr-flags"))
@api.funkify
def download_dataset(dag):
    from sklearn.datasets import load_iris  # pyright:ignore[reportMissingImports] # noqa:F401

    return load_iris(as_frame=True).frame.dropna()


@api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("dkr-flags"))
@api.funkify
def predict_target(dag, dataset_uri):
    import io

    import pandas as pd  # pyright:ignore[reportMissingImports] # noqa:F401
    from sklearn.linear_model import LogisticRegression  # pyright:ignore[reportMissingImports] # noqa:F401

    from daggerml.contrib.s3 import S3Store

    payload = S3Store().get(dataset_uri.value())
    df = pd.read_parquet(io.BytesIO(payload))
    features = df.drop(columns=["target"])
    target = df["target"]
    model = LogisticRegression(max_iter=200)
    model.fit(features, target)
    out = df.copy()
    out["prediction"] = model.predict(features)
    return out


def _docker_run_flags() -> list[str]:
    flags: list[str] = []
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        parsed = urlparse(endpoint)
        if parsed.scheme == "http" and parsed.port is not None:
            flags.extend(
                [
                    "--add-host=host.docker.internal:host-gateway",
                    "-e",
                    f"AWS_ENDPOINT_URL=http://host.docker.internal:{parsed.port}",
                ]
            )
    for key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    ):
        value = os.environ.get(key)
        if value:
            flags.extend(["-e", f"{key}={value}"])
    return flags


def main() -> None:
    moto_server = _start_local_moto_if_needed()
    flags = _docker_run_flags()
    try:
        import pandas  # pyright:ignore[reportMissingImports] # noqa:F401

        raise RuntimeError("pandas should not be installed in the local environment for this example to work")
    except ModuleNotFoundError:
        pass
    try:
        with Dml.temporary() as dml:
            with dml.new("examples/01-docker-dataset") as dag:
                dag.dkr_build = docker_build
                s3 = S3Store()
                print("Creating Docker build context from repo root, excluding patterns:", EXCLUDE_PATTERNS)
                dkr_ctx = s3.tar(str(REPO_ROOT), excludes=EXCLUDE_PATTERNS, symlinks="ignore")
                dag.put(flags, name="dkr-flags")
                print("Building Docker image (this may take a moment)...")
                t0 = time()
                dag.dkr_build(dkr_ctx, build_flags=["-f", "./examples/dkr-ctx/Dockerfile"], name="image")
                t1 = time()
                print("Re-building Docker image to demonstrate caching...")
                t2 = time()
                dag.dkr_build(dkr_ctx, build_flags=["-f", "./examples/dkr-ctx/Dockerfile"], name="image-redux")
                t3 = time()
                dag.download = download_dataset
                print("Loading dataset within Docker...")
                dataset = dag.download(name="dataset")
                print("Training model and generating predictions within Docker...")
                predictions = dag.call(predict_target, dataset, name="predictions")
                print("Committing DAG to persist artifacts...")
                dag.commit(predictions)
            print("Reading predictions parquet from S3...")
            df = pl.read_parquet(predictions.value().uri)
            print(f"Dataset parquet URI: {dataset.value()}")
            print(f"\nPredictions parquet URI: {predictions.value().uri}")
    finally:
        if moto_server is not None:
            moto_server.stop()
    print("\nPredictions:")
    print(df.head())
    print(f"\nBuild times: {t1 - t0:.2f}s (cached: {t3 - t2:.2f}s)")


if __name__ == "__main__":
    main()
