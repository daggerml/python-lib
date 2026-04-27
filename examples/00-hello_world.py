"""Run a minimal local-script hello world through `@api.funkify`.

This example executes a simple funkified Python function with the local script
runtime using an already configured remote URI.
"""

from __future__ import annotations

import os

from daggerml import Dml
from daggerml.contrib import api


def _require_remote_uri() -> None:
    if os.environ.get("DML_REMOTE_URI"):
        return
    raise RuntimeError(
        "DML_REMOTE_URI is required. Set remote env vars first (for local moto, run examples/moto_server_env.py)."
    )


@api.funkify(uri="script", adapter="local")
def hello(dag):
    from uuid import uuid4

    return f"Hello, world! Your UUID is {uuid4()}"


def main() -> None:
    _require_remote_uri()
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


if __name__ == "__main__":
    main()
