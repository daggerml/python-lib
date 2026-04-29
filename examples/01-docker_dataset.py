"""Run an end-to-end Docker-backed dataset pipeline.

The example builds a Docker image from this repository, loads the iris dataset
in one Docker-executed funk, and trains a small classifier in another. It
exercises the contrib runtime end to end: script funkification, Docker
execution, remote cache publication, and S3-backed artifact exchange between
DAG nodes.
"""

from __future__ import annotations

import os
from pathlib import Path
from time import time
from urllib.parse import urlparse

import polars as pl

import daggerml as dml
from daggerml.contrib import api
from daggerml.contrib.funks import docker_build
from daggerml.contrib.s3 import S3Store

EXCLUDE_PATTERNS = (
    # ".git",  # we need .git to install lib from the repo
    "ignore/*",
    ".venv/*",
    ".mypy_cache/*",
    ".pytest_cache/*",
    "__pycache__/*",
    "*.pyc",
    "tests/*",
)
REPO_ROOT = Path(__file__).resolve().parents[1]


@api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("dkr-flags"))
@api.funkify
def download_dataset(dag):
    from sklearn.datasets import load_iris  # pyright:ignore[reportMissingImports] # noqa:F401

    return load_iris(as_frame=True).frame.dropna()


@api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("dkr-flags"))
@api.funkify
def predict_target(dag, dataset_uri, params):
    import io

    import pandas as pd  # pyright:ignore[reportMissingImports] # noqa:F401
    from sklearn.linear_model import LogisticRegression  # pyright:ignore[reportMissingImports] # noqa:F401

    from daggerml.contrib.s3 import S3Store

    payload = S3Store().get(dataset_uri.value())
    df = pd.read_parquet(io.BytesIO(payload))
    features = df.drop(columns=["target"])
    target = df["target"]
    model = LogisticRegression(max_iter=200, **params.value())
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
    flags = _docker_run_flags()
    try:
        import pandas  # pyright:ignore[reportMissingImports] # noqa:F401

        raise RuntimeError("pandas should not be installed in the local environment for this example to work")
    except ModuleNotFoundError:
        pass
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
        dag.predict_fn = predict_target
        predictions = dag.call(predict_target, dataset, {"l1_ratio": 0.2}, name="predictions")
        print("Committing DAG to persist artifacts...")
        dag.commit(predictions)
    print("Reading predictions parquet from S3...")
    df = pl.read_parquet(predictions.value().uri)
    print(f"Dataset parquet URI: {dataset.value()}")
    print(f"\nPredictions parquet URI: {predictions.value().uri}")
    print("\nPredictions:")
    print(df.head())
    print(f"\nBuild times: {t1 - t0:.2f}s (cached: {t3 - t2:.2f}s)")


if __name__ == "__main__":
    main()
