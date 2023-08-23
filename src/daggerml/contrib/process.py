#!/usr/bin/env python
import inspect
from functools import partial, wraps
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from textwrap import dedent
from time import sleep
from uuid import uuid4

from daggerml._dag import Dag
from daggerml.contrib.s3 import s3_upload
from daggerml.contrib.util import fullname, local_executor


def local_fn(fn=None, executor_name=None, executor_version=None, env=None, env_type=None):
    """wraps a function into a node"""
    if fn is None:
        return partial(local_fn, executor_name=executor_name, executor_version=executor_version,
                       env=env, env_type=env_type)
    @wraps(fn)
    def wrapped(dag, *args, **meta):
        if executor_name is None:
            executor, secret = dag.executor, dag.secret
        else:
            executor, secret = local_executor(executor_name, executor_version).get(dag)
        node = dag.from_py([executor, fullname(fn)]).call_async(*args, **meta)
        while True:
            fn_dag = Dag.from_claim(executor, secret, ttl=-1, node_id=node.id)
            if isinstance(fn_dag, Dag):
                break
            sleep(0.1)
        with fn_dag:
            _, _, *args = fn_dag.expr
            fn_dag.commit(fn(dag, *args))
        return node.wait()
    return wrapped


def process_fn(fn=None, env=None, env_type=None):
    """wraps a function into a node -- you probably want to wrap this in `local_fn`

    Doesn't work yet
    """
    if fn is None:
        return partial(local_fn, env=env, env_type=env_type)
    @wraps(fn)
    def wrapped(dag, *args, **meta):
        import requests_auth_aws_sigv4 as req

        import daggerml as dml
        src = s3_upload(dag, dedent(inspect.getsource(fn)).encode())
        # from tempfile import NamedTemporaryFile, TemporaryDirectory
        node = dag.from_py([dag.executor, src, fullname(fn)]).call_async(*args, **meta)
        if node.check() is not None:
            return node.result
        with TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            (tmpdir / 'daggerml').symlink_to(Path(dml.__file__).parent)
            (tmpdir / 'requests_auth_aws_sigv4').symlink_to(Path(req.__file__).parent)
            with open(tmpdir / str(uuid4()), 'wb') as fo:
                fo.write('from daggerml.contrib.process import process_fn')
                fo.write(src)
        while True:
            fn_dag = Dag.from_claim(dag.executor, dag.secret, ttl=-1, node_id=node.id)
            if isinstance(fn_dag, Dag):
                break
            sleep(0.1)
        with fn_dag:
            _, _, *args = fn_dag.expr
            fn_dag.commit(fn(dag, *args))
        return node.wait()
    return wrapped
