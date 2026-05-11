"""Run an end-to-end Docker-backed dataset pipeline.

The example builds a Docker image from this repository, loads the iris dataset
in one Docker-executed funk, and trains a small classifier in another. It
exercises the contrib runtime end to end: script funkification, Docker
execution, remote cache publication, and S3-backed artifact exchange between
DAG nodes.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from time import time
from urllib.parse import urlparse

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


@api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("dkr-flags"))
@api.funkify
def download_dataset(dag):
    from sklearn.datasets import load_iris  # pyright:ignore[reportMissingImports] # noqa:F401

    return load_iris(as_frame=True).frame.dropna()


@api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("dkr-flags"))
@api.funkify
def predict_target(dag, dataset, params):
    import pandas as pd  # pyright:ignore[reportMissingImports] # noqa:F401
    from sklearn.linear_model import LogisticRegression  # pyright:ignore[reportMissingImports] # noqa:F401
    from sklearn.metrics import r2_score  # pyright:ignore[reportMissingImports] # noqa:F401
    from sklearn.model_selection import train_test_split  # pyright:ignore[reportMissingImports] # noqa:F401

    df = pd.read_parquet(dataset.value().uri)
    X = df.drop(columns=["target"])
    y = df["target"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=42)
    model = LogisticRegression(**params.value())
    model.fit(X_train, y_train)
    train_r2 = r2_score(y_train, model.predict(X_train))
    test_r2 = r2_score(y_test, model.predict(X_test))
    return {"train": train_r2, "test": test_r2}


def main() -> None:
    flags = _docker_run_flags()
    try:
        import pandas  # pyright:ignore[reportMissingImports] # noqa:F401

        raise RuntimeError("pandas should not be installed in the local environment for this example to work")
    except ModuleNotFoundError:
        pass
    with dml.new(name="examples/01-docker-dataset") as dag:
        dag.dkr_build = docker_build
        s3 = S3Store()
        print("Creating Docker build context from repo root, excluding patterns:", EXCLUDE_PATTERNS)
        dkr_ctx = s3.tar(str(REPO_ROOT), excludes=EXCLUDE_PATTERNS, symlinks="ignore")
        dag.put(flags, name="dkr-flags")
        print("Building Docker image (this may take a moment)...")
        t0 = time()
        dag.dkr_build(dkr_ctx, build_flags=["-f", "./examples/dkr-ctx/Dockerfile"], name="image")
        t1 = time()
        dag.download = download_dataset
        print("Loading dataset within Docker...")
        dataset = dag.download(name="dataset")
        print("Training model and generating predictions within Docker...")
        dag.predict_fn = predict_target
        predictions = dag.predict_fn(
            dataset,
            {"max_iter": 200, "solver": "saga", "penalty": "elasticnet", "l1_ratio": 0.2},
            name="predictions",
        )
        print("Committing DAG to persist artifacts...")
        dag.commit(predictions)
    print("Reading predictions parquet from S3...")
    print(json.dumps(predictions.value(), indent=2))
    print(f"\nBuild time: {t1 - t0:.2f}s")


if __name__ == "__main__":
    main()
