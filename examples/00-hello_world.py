"""Run a minimal local-script hello world through `@api.funkify`.

This example executes a simple funkified Python function with the local script
runtime. If `DML_REMOTE_ROOT` is not already configured, it starts a local moto
S3 server so the example can run end to end without external infrastructure.
"""

from __future__ import annotations

import os
from typing import Any

import boto3

from daggerml import Dml
from daggerml.contrib import api


def _start_local_moto_if_needed() -> Any | None:
    if os.environ.get("DML_REMOTE_ROOT"):
        return None
    try:
        for key in list(os.environ.keys()):
            if key.startswith("AWS_"):
                del os.environ[key]
        os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
        from moto.server import ThreadedMotoServer
    except ModuleNotFoundError as e:
        raise RuntimeError("Set DML_REMOTE_ROOT for a real S3 bucket, or install moto[server] for local dev.") from e
    server = ThreadedMotoServer(port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    os.environ.setdefault("AWS_REGION", "us-east-1")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ["AWS_ENDPOINT_URL"] = endpoint
    os.environ["DML_REMOTE_ROOT"] = "s3://daggerml-example/hello-world"
    boto3.client("s3", endpoint_url=endpoint).create_bucket(Bucket="daggerml-example")
    return server


@api.funkify(uri="script", adapter="local")
def hello(dag):
    from uuid import uuid4

    return f"Hello, world! Your UUID is {uuid4()}"


def main() -> None:
    moto_server = _start_local_moto_if_needed()
    try:
        with Dml.temporary() as dml:
            with dml.new("examples/00-hello-world") as dag:
                result = dag.call(hello, name="greeting")
                dag.commit(result)
            loaded = dml.load("examples/00-hello-world")
            print(loaded.result.value())

        # Run the same DAG again to demonstrate loading from cache
        # Note that this is a brand new Dml DB so there's no local cache
        with Dml.temporary() as dml:
            with dml.new("examples/00-hello-world") as dag:
                print(dag.call(hello, name="greeting").value())
    finally:
        if moto_server is not None:
            moto_server.stop()


if __name__ == "__main__":
    main()
