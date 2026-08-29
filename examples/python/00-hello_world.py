"""Run a minimal local-script hello world through `@api.funkify`.

This example executes a simple funkified Python function with the local script
runtime using an already configured remote URI.
"""

from __future__ import annotations

import argparse

import daggerml as dml
from daggerml.contrib import api


@api.funkify
def hello(dag, arg):
    print("hello: generating greeting")
    from uuid import uuid4

    arg = arg.value()
    return f"{uuid4() = !s} and {arg = }."


def main(dag_name: str) -> None:
    dag = dml.new(name=dag_name)
    dag.hello_fn = hello
    result = dag.call(hello, 23, name="greeting")
    dag.commit(result)
    loaded = dml.load(dag_name)
    print(loaded.result.value())
    dag = dml.new(name=f"{dag_name}/redux")
    print(dag.call(hello, 23).value())
    print(dag.call(hello, 42).value())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dag_name")
    main(parser.parse_args().dag_name)
