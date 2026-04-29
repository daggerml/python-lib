"""Run a minimal local-script hello world through `@api.funkify`.

This example executes a simple funkified Python function with the local script
runtime using an already configured remote URI.
"""

from __future__ import annotations

import daggerml as dml
from daggerml.contrib import api


@api.funkify(uri="script", adapter="local")
def hello(dag):
    from uuid import uuid4

    return f"Hello, world! Your UUID is {uuid4()}"


def main() -> None:
    with dml.new("examples/00-hello-world") as dag:
        result = dag.call(hello, name="greeting")
        dag.commit(result)
    loaded = dml.load("examples/00-hello-world")
    print(loaded.result.value())
    with dml.new("examples/00-hello-world") as dag:
        print(dag.call(hello, name="greeting").value())


if __name__ == "__main__":
    main()
