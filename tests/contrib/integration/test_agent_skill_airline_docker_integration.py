"""Run the installed authoring example through a real Docker/Moto pipeline."""

import importlib
import json
import math
import os
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import pytest

import daggerml.api as api
from daggerml.contrib.codecs import DelayedActionCodec
from daggerml.contrib.s3 import S3Store

pytestmark = [pytest.mark.slow, pytest.mark.docker, pytest.mark.serial]


@pytest.fixture
def airline_image(tmp_path):
    if not shutil.which("docker"):
        pytest.skip("Docker is required")
    try:
        available = subprocess.run(["docker", "info"], capture_output=True, timeout=10).returncode == 0
    except subprocess.TimeoutExpired:
        available = False
    if not available:
        pytest.skip("Docker daemon is not available")

    root = Path(__file__).resolve().parents[3]
    dockerfile = tmp_path / "Dockerfile"
    base = (root / "docs/Dockerfile").read_text(encoding="utf-8")
    # sklearn provides glibc wheels; use Debian for both stages rather than
    # mixing an Alpine-built DaggerML extension into a glibc runtime.
    base = base.replace("python:3.13-alpine", "python:3.13-slim")
    base = base.replace(
        "apk add --no-cache build-base cmake", "apt-get update && apt-get install -y build-essential cmake"
    )
    dockerfile.write_text(base + "\nRUN python -m pip install --no-cache-dir polars scikit-learn\n", encoding="utf-8")
    image = f"dml-airline-example:{uuid4().hex}"
    try:
        subprocess.run(["docker", "build", "-f", str(dockerfile), "-t", image, str(root)], check=True, timeout=600)
        yield image
    finally:
        subprocess.run(["docker", "image", "rm", "-f", image], capture_output=True, check=False, timeout=30)


def test_airline_dagclass_search_runs_in_docker(runtime_world, airline_image, monkeypatch):
    module = importlib.import_module("daggerml._core.skills.authoring.examples.dagclass")
    monkeypatch.setattr(
        api, "_codecs",
        [(1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()), (0, 3, api.MiscPyTypeCodec())],
    )

    endpoint = urlparse(os.environ["AWS_ENDPOINT_URL"])
    flags = ["--add-host=host.docker.internal:host-gateway", "-e",
             f"AWS_ENDPOINT_URL=http://host.docker.internal:{endpoint.port}"]
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "AWS_DEFAULT_REGION"):
        flags.extend(["-e", f"{key}={os.environ[key]}"])

    # The same date, distance and delay schema as Vega flights-2k.json;
    # deterministic local data keeps the Docker test independent of GitHub.
    rows = [
        {"date": f"2001/01/{1 + index // 24:02d} {index % 24:02d}:00",
         "distance": 100 + 20 * (index % 31),
         "delay": (index % 24) * 2 + (index % 7) * 3 + index % 5}
        for index in range(240)
    ]
    with api.use_default_dml(runtime_world.publisher):
        with api.new("airline-seed") as seed:
            seed.commit(seed.put("initialize remote before staging data"))
        source = S3Store().put(data=json.dumps(rows).encode("utf-8"), suffix=".json")
        best = module.run(airline_image, source, flags=flags, name="airline-delay-smoke")
        parquet_cuts = [item for item in S3Store().ls(recursive=True) if item.uri.endswith(".parquet")]

    result = api.load("airline-delay-smoke", dml=runtime_world.publisher).result.value()
    assert result == best
    assert len(parquet_cuts) == 2
    assert set(best) == {"params", "objective"}
    assert best["params"]["max_depth"] in (3, 7)
    assert best["params"]["min_samples_leaf"] in (5, 20)
    assert math.isfinite(best["objective"])
    assert best["objective"] > 0
