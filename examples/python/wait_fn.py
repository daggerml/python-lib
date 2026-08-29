"""Run a minimal local-script hello world through `@api.funkify`.

This example executes a simple funkified Python function with the local script
runtime using an already configured remote URI.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor

import daggerml as dml
from daggerml.contrib import api


@api.funkify
def hello(dag, arg, fn=None):
    print("hello: starting delayed greeting")
    from random import Random
    from time import sleep

    # set random seed for reproducibility
    resp = n = arg.value()
    rng = Random(42 + n)
    if fn is not None:
        print(f"Running {fn} with arg {2 * n}")
        resp = fn(5 * n).value()
    sleep(rng.random() + n)
    return f"Hello, {resp}!"


def main(dag_name: str) -> None:
    dag = dml.new(name=dag_name)
    dag.hello_fn = hello
    with ThreadPoolExecutor() as executor:
        future1 = executor.submit(dag.call, hello, 0.4, dag.hello_fn, name="hello-23")
        future2 = executor.submit(dag.call, hello, 0.3, dag.hello_fn, name="hello-42")
        for f in [future1, future2]:
            try:
                print(f.result())
            except dml.CanceledExecutionError:
                print("Task was cancelled.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dag_name")
    main(parser.parse_args().dag_name)
