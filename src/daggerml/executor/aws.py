#!/usr/bin/env python3
import asyncio
import inspect
import json
import os
from contextlib import contextmanager
from dataclasses import InitVar, dataclass, field
from datetime import datetime, timezone
from tempfile import NamedTemporaryFile
from textwrap import dedent
from uuid import uuid4

import boto3
import botocore.session

import daggerml as dml
from daggerml.executor import ExecutionEnvironment

dml_root = os.path.dirname(dml.__file__)


def now():
    return datetime.now(timezone.utc)

@dataclass
class AwsBase(ExecutionEnvironment):
    aws_access_key_id: InitVar[str|None] = field(kw_only=True, default=None)
    aws_secret_access_key: InitVar[str|None] = field(kw_only=True, default=None)
    aws_session_token: InitVar[str|None] = field(kw_only=True, default=None)
    region_name: InitVar[str|None] = field(kw_only=True, default=None)
    botocore_session: InitVar[botocore.session.Session|None] = field(kw_only=True, default=None)
    session: boto3.Session = field(init=False)

    def __post_init__(self, **kw):
        self.session = boto3.Session(**kw)

@dataclass
class S3Base(AwsBase):
    bucket: str
    prefix: str = ''

    def cat(self, loc):
        s3 = self.session.client('s3')
        obj = s3.get_object(Bucket=self.bucket, Key=f'{self.prefix}/{loc}')
        return obj['Body'].read().decode()

    def put_obj(self, obj: bytes, dst: str):
        s3 = self.session.client('s3')
        s3.put_object(Body=obj, Bucket=self.bucket, Key=f'{self.prefix}/{dst}')
        return dst

    async def acall(self, cmd):
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            stderr = stderr.decode()
            msg = f'proc command failed unexpectedly\n\n{stderr}'
            context = {'stderr': stderr}
            code = 'infra'
            raise dml.Error(msg, context=context, code=code)
        return stdout

    def call(self, cmd):
        return asyncio.run(self.acall(cmd))

    @contextmanager
    def tmpdir(self):
        tmpd = f'{now().isoformat()}-{uuid4().hex[:8]}'
        try:
            yield tmpd
        finally:
            s3 = self.session.resource('s3')
            s3.Bucket(self.bucket).objects.filter(Prefix=f'{self.prefix}/{tmpd}/').delete()

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
class BaseExecutor:
    cmd_tpl: str
    exec_env: ExecutionEnvironment

    def prepare(self, fn_dag, resp_file, **kw):
        raise NotImplementedError('BaseExecutor should be subclassed')

    @property
    def meta(self):
        return {
            'executor': type(self).__name__,
            'command-template': self.cmd_tpl,
        }

    def run(self, dag, args, cache: bool = True, retry: bool = False, **kw) -> dml.Node:
        resource = dml.Resource(json.dumps(self.meta, separators=(',', ':'), sort_keys=True))
        args = [x if isinstance(x, dml.Node) else dag.put(x) for x in args]
        fndag = dag.start_fn(dag.put(resource), *args, cache=cache, retry=retry)
        if isinstance(fndag, dml.Node):
            return fndag
        with self.exec_env.tmpdir() as tmpd:
            resp_file = f'{tmpd}/result'
            for k, v in self.prepare(fndag, resp_file, **kw):
                self.exec_env.put_obj(v, f'{tmpd}/{k}')
            self.exec_env.call(self.cmd_tpl.format(tmpd))
            result = self.exec_env.cat(resp_file)
        result = dml.from_json(result)
        if isinstance(result, dml.Error):
            raise result
        node = fndag.commit(fndag.put(result))
        return node

@dataclass
class PyExecutor(BaseExecutor):
    python: str
    cmd_tpl: str = field(init=False)

    def __post_init__(self):
        self.cmd_tpl = f'{self.python!r} "{{0}}/script.py" "{{0}}/args.json"'

    @staticmethod
    def make_script(fnname, src, result_file):
        tpl = dedent(
            """
            import sys
            try:
                import daggerml as dml
            except ImportError:
                import dml_copy as dml

            with open(sys.argv[1], 'r') as f:
                expr = dml.from_json(f.read())

            {src}

            if __name__ == '__main__':
                try:
                    result = {fnname}(*expr)
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    result = dml.Error.from_ex(e)
                import json
                with open({result_file!r}, 'w') as f:
                    json.dump(dml.to_data(result), f)
            """
        )
        return tpl.format(
            src=dedent(src),
            fnname=fnname,
            result_file=result_file,
        )

    def prepare(self, fn_dag: dml.FnDag, result_file: str):
        yield 'args.json', dml.to_json(fn_dag.expr[2:]).encode()
        script = self.make_script(*fn_dag.expr[1], result_file)
        yield 'script.py', script.encode()
        with open(f'{dml_root}/core.py', 'rb') as f:
            yield 'dml_copy.py', f.read()

    def call(self, dag, fn, *args: dml.Node, cache: bool = True):
        src = inspect.getsource(fn)
        resp = self.run(dag, [[fn.__name__, src], *args], cache=cache)
        return resp
