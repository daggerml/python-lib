"""Run a minimal local-script hello world through `@api.funkify`.

This example executes a simple funkified Python function with the local script
runtime using an already configured remote URI.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import daggerml as dml
from daggerml.contrib import api


@api.funkify
def hello(dag, arg, fn=None):
    from random import Random
    from time import sleep

    # set random seed for reproducibility
    resp = n = arg.value()
    rng = Random(42 + n)
    sleep(rng.random() + n)
    if fn is not None:
        print(f"Running {fn} with arg {2 * n}")
        resp = fn(3 * n).value()
    sleep(rng.random() + n)
    return f"Hello, {resp}!"


if __name__ == "__main__":
    dag = dml.new(name="wait_fn")
    dag.hello_fn = hello
    with ThreadPoolExecutor() as executor:
        future1 = executor.submit(dag.call, hello, 0.4, dag.hello_fn, name="hello-23")
        future2 = executor.submit(dag.call, hello, 0.3, dag.hello_fn, name="hello-42")
        try:
            dag.commit([future1.result(), future2.result()])
        except dml.CancelledError:
            pass
