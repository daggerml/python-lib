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
    from random import random
    from time import sleep

    sleep(random() * 2 + 0)
    resp = arg
    if fn is not None:
        print(f"Running {fn} with arg {arg}")
        resp = fn(arg)
    sleep(random() * 2 + 0)
    return f"Hello, {resp}!"


if __name__ == "__main__":
    dag = dml.new(name="wait_fn")
    dag.hello_fn = hello
    with ThreadPoolExecutor() as executor:
        future1 = executor.submit(dag.call, hello, 23, dag.hello_fn, name="hello-23")
        future2 = executor.submit(dag.call, hello, 42, dag.hello_fn, name="hello-42")
        dag.commit([future1.result(), future2.result()])
