"""Run a minimal local-script hello world through `@api.funkify`.

This example executes a simple funkified Python function with the local script
runtime using an already configured remote URI.
"""

from __future__ import annotations

import daggerml as dml
from daggerml.contrib import api


@api.funkify
def hello(dag, arg):
    from uuid import uuid4

    arg = arg.value()
    return f"{uuid4() = !s} and {arg = }."


def main() -> None:
    dag = dml.new(name="examples/00-hello-world")
    dag.hello_fn = hello
    result = dag.call(hello, 23, name="greeting")
    dag.commit(result)
    loaded = dml.load("examples/00-hello-world")
    print(loaded.result.value())
    dag = dml.new(name="examples/00-hello-world-redux")
    print(dag.call(hello, 23).value())
    print(dag.call(hello, 42).value())


if __name__ == "__main__":
    main()
