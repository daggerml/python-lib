#!/usr/bin/env python3
import asyncio
import inspect
import os
import shutil
from contextlib import contextmanager
from dataclasses import InitVar, dataclass, field
from tempfile import NamedTemporaryFile, TemporaryDirectory
from textwrap import dedent
from typing import Callable, Dict
from uuid import uuid4

import paramiko

import daggerml as dml


@dataclass
class ExecutionEnvironment:
    def cat(*args, **kwargs):
        raise NotImplementedError('cat has not been implemented')
    def put_obj(*args, **kwargs):
        raise NotImplementedError('put_obj has not been implemented')
    def call(*args, **kwargs):
        raise NotImplementedError('call has not been implemented')
    @contextmanager
    def tmpdir(*args, **kwargs):
        raise NotImplementedError('tmpdir has not been implemented')

@dataclass
class Local(ExecutionEnvironment):

    def cat(self, loc):
        with open(loc, 'r') as f:
            return f.read()

    def put(self, src: str|bytes, dst: str):
        shutil.copyfile(src, dst)
        return dst

    def put_obj(self, obj: bytes, dst: str):
        with open(dst, 'wb') as f:
            f.write(obj)
        return dst

    async def acall(self, cmd):
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            msg = 'proc command failed unexpectedly'
            context = {'stderr': stderr}
            code = 'infra'
            raise dml.Error(msg, context=context, code=code)
        return stdout

    def call(self, cmd):
        return asyncio.run(self.acall(cmd))

    @contextmanager
    def tmpdir(self):
        with TemporaryDirectory() as tmpd:
            yield tmpd

@dataclass
class RemoteSsh(ExecutionEnvironment):
    user: str
    host: str
    port: int
    key: str

    def __post_init__(self):
        if self.key is not None:
            self.key = os.path.expanduser(self.key)

    @contextmanager
    def ssh(self):
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.connect(self.host, port=self.port, username=self.user, key_filename=self.key)
        try:
            yield client
        finally:
            client.close()

    @contextmanager
    def sftp(self):
        with self.ssh() as ssh:
            try:
                sftp = ssh.open_sftp()
                yield sftp
            finally:
                sftp.close()

    def get(self, remote_loc, local_loc):
        with self.sftp() as sftp:
            sftp.get(remote_loc, local_loc)
        return local_loc

    def cat(self, remote_loc):
        with NamedTemporaryFile() as tmpf:
            self.get(remote_loc, tmpf.name)
            with open(tmpf.name) as f:
                return f.read()

    def put(self, src: str|bytes, dst: str):
        with self.sftp() as sftp:
            sftp.put(src, dst)
        return dst

    def put_obj(self, obj: bytes, dst: str):
        with NamedTemporaryFile() as tmpf:
            with open(tmpf.name, 'wb') as f:
                f.write(obj)
            return self.put(tmpf.name, dst)

    def call(self, cmd):
        with self.ssh() as ssh:
            stdin, stdout, stderr = ssh.exec_command(cmd)
            return stdout.read()

    @contextmanager
    def tmpdir(self):
        tmpd = None
        try:
            tmpd = self.call('mktemp -d')
            yield tmpd
        finally:
            if tmpd is not None:
                self.call(f'rm -rf {tmpd}')

@dataclass
class Formatter:
    tpl: str
    func: InitVar[Callable[..., str]|None] = None
    _func: Callable[..., str] = field(init=False)

    def __post_init__(self, func):
        self._func = func or self.tpl.format

    def __call__(self, txt: str) -> str:
        return self._func(txt)

    def __str__(self):
        return self.tpl

@dataclass
class BaseExecutor:
    cmd_tpl: Formatter
    exec_env: ExecutionEnvironment

    def prepare(self, fn_dag, resp_file, **kw):
        raise NotImplementedError('BaseExecutor should be subclassed')

    def run(self, dag, args, cache: bool = True, **kw):
        resource = dml.Resource({
            'executor': type(self).__name__,
            'command-template': str(self.cmd_tpl),
        })
        args = [x if isinstance(x, dml.Node) else dag.put(x) for x in args]
        fn_dag = dag.start_fn(dag.put(resource), *args)
        with self.exec_env.tmpdir() as tmpd:
            resp_file = f'{tmpd}/result'
            for k, v in self.prepare(fn_dag, resp_file, **kw):
                self.exec_env.put_obj(v, f'{tmpd}/{k}')
            self.exec_env.call(self.cmd_tpl(tmpd))
            result = self.exec_env.cat(resp_file)
        result = dml._util.from_json(result)
        if isinstance(result, dml.Error):
            raise result
        node = fn_dag.commit(fn_dag.put(result))
        return node

@dataclass
class PyExecutor(BaseExecutor):
    python: str
    cmd_tpl: Formatter = field(init=False)

    def __post_init__(self):
        self.cmd_tpl = Formatter(f'{self.python!r} {{}}/script.py')

    @staticmethod
    def make_script(expr, result_file, preamble=()):
        fnname, src = expr[1].unroll()
        out = [*preamble, '', 'import daggerml as dml', '', dedent(src), '']
        out.append(dedent(f"""
        if __name__ == '__main__':
            try:
                expr = {[dml._util.to_data(x.unroll()) for x in expr[2:]]!r}
                expr = [dml._util.from_data(x) for x in expr]
                result = {fnname}(*expr)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                result = dml.Error.from_ex(e)
            import json
            with open({result_file!r}, 'w') as f:
                json.dump(dml._util.to_data(result), f)
        """))
        return '\n'.join(out)

    def prepare(self, fn_dag, result_file, preamble=()):
        script = self.make_script(fn_dag.expr, result_file, preamble)
        yield 'script.py', script.encode()

    def call(self, dag, fn, *args: dml.Node, cache: bool = True):
        src = inspect.getsource(fn)
        resp = self.run(dag, [[fn.__name__, src], *args], cache=cache)
        return resp

@dataclass
class CondaPyExecutor(PyExecutor):

    def __post_init__(self):
        self.cmd_tpl = Formatter(f'conda run -n {self.python!r} python "{{}}/script.py"')

@dataclass
class HatchPyExecutor(PyExecutor):

    def __post_init__(self):
        self.cmd_tpl = Formatter(f'hatch -e {self.python!r} run python "{{}}/script.py"')

@dataclass
class InProcEnv(ExecutionEnvironment):
    store: Dict = field(default_factory=dict)

    def cat(self, loc):
        store_key, obj_key = loc.split('/', 1)
        return self.store[store_key][obj_key]

    def put_obj(self, obj: bytes, dst: str):
        store_key, obj_key = dst.split('/', 1)
        self.store[store_key][obj_key] = obj
        return dst

    def call(self, store_id):
        fn = self.store[store_id]['fn']
        args = self.store[store_id]['args']
        args = [x.unroll() for x in args]
        resp = fn(*args)
        result_key = self.store[store_id]['result']
        self.put_obj(dml._util.to_json(resp).encode(), result_key)
        return resp

    @contextmanager
    def tmpdir(self):
        _id = uuid4().hex
        self.store[_id] = {}
        yield _id
        del self.store[_id]

@dataclass
class InProcExecutor(BaseExecutor):
    cmd_tpl: Formatter = field(default=Formatter('{}'))
    exec_env: ExecutionEnvironment = field(default_factory=InProcEnv)

    def prepare(self, fn_dag, result_file, fn):
        yield 'fn', fn
        yield 'args', fn_dag.expr[2:]
        yield 'result', result_file

    def call(self, dag, fn, *args: dml.Node, cache: bool = True):
        src = inspect.getsource(fn)
        resp = self.run(dag, [[fn.__name__, src], *args], cache=cache, fn=fn)
        return resp
