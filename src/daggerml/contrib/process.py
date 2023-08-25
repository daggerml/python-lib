#!/usr/bin/env python
import hashlib
import importlib.util
import inspect
import json
import os
import subprocess
import sys
from base64 import b64decode, b64encode
from functools import partial, wraps
from textwrap import dedent

from daggerml._dag import Dag, json_dumps
from daggerml.contrib.util import cached_executor, fullname


def local_fn(fn=None, executor_name=None, executor_version=None):
    """wraps a function into a node"""
    if fn is None:
        return partial(local_fn, executor_name=executor_name, executor_version=executor_version)
    @wraps(fn)
    def wrapped(dag, *args, **meta):
        executor, secret = cached_executor(executor_name, executor_version).get(dag)
        _hash = hashlib.md5(dedent(inspect.getsource(fn)).encode()).hexdigest()
        print('hash ==', _hash, *args)
        node = dag.from_py([executor, _hash])
        node = node.call_async(*args, **meta, dml_name=fullname(fn))
        if node.result:
            return node.result
        with Dag.from_claim(executor, secret, ttl=-1, node_id=node.id) as fn_dag:
            _, _, *args = fn_dag.expr
            fn_dag.commit(fn(fn_dag, *args))
        return node.wait()
    return wrapped


def hatch(fn=None, env=None, executor_name=None, executor_version=None):
    """executes a function in a different venv -- you probably want to wrap this in `local_fn`
    """
    if fn is None:
        return partial(hatch, env=env, executor_name=executor_name, executor_version=executor_version)
    @wraps(fn)
    def wrapped(dag, *args, **meta):
        if os.getenv('DML_EXECUTING'):
            return fn(dag, *args)
        executor, secret = cached_executor(executor_name, executor_version).get(dag)
        src = hashlib.md5(dedent(inspect.getsource(fn)).encode()).hexdigest()
        pip = subprocess.run(f'hatch -e {env} run python -m pip freeze',
                             shell=True, check=True, capture_output=True)
        pip = hashlib.md5(pip.stdout.strip()).hexdigest()
        node = dag.from_py([dag.executor, f'{pip}:{src}']) \
            .call_async(*args, **meta, dml_name=fullname(fn), dml_file=fn.__globals__['__file__'])
        if node.result:
            return node.result
        with Dag.from_claim(executor, secret, ttl=-1, node_id=node.id) as fn_dag:
            resp = subprocess.run(f'hatch -e {env} run python {__file__}',
                                  shell=True, capture_output=True,
                                  env=dict(DML_DAG=b64encode(json_dumps(vars(fn_dag)).encode()).decode(),
                                           DML_EXECUTING='1',
                                           **os.environ))
            if resp.returncode != 0:
                fn_dag.fail({
                    'message': 'subproccess failed',
                    'returncode': resp.returncode,
                    'stdout': resp.stdout.decode(),
                    'stderr': resp.stderr.decode(),
                })
        return node.wait()
    return wrapped


def import_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


if __name__ == '__main__':
    dag = b64decode(os.getenv('DML_DAG').encode()).decode()
    with Dag(**json.loads(dag)) as dag:
        _, _, *args = dag.expr
        meta = dag.expr.meta
        script = import_file('daggerml.contrib.script', meta['dml_file'])
        # FIXME hack because imports aren't done correctly
        *_, fnname = meta['dml_name'].split('.')
        fn = getattr(script, fnname)
        dag.commit(fn(dag, *args))
