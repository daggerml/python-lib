import asyncio
import gzip
import inspect
import json
import logging
import platform
import re
import subprocess
import tarfile
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Any, Callable, List
from urllib.parse import urlparse
from uuid import uuid4

import boto3

import daggerml as dml

try:
    import pandas as pd
    import pandas.util  # noqa: F401
except ImportError:
    pd = None

try:
    import polars as pl
except ImportError:
    pl = None

logger = logging.getLogger(__name__)


@dataclass
class Lambda:
    @staticmethod
    def run(dag, expr, boto_session=boto3):
        expr = [dag.put(x) for x in expr]
        waiter = dag.start_fn(*expr)
        resource, *_ = expr
        lambda_arn = resource.value().uri

        def lambda_update_fn(cache_key, dump, reqs):
            resp = boto_session.client('lambda').invoke(
                FunctionName=lambda_arn,
                Payload=json.dumps({'dump': dump, 'cache_key': cache_key}).encode()
            )
            payload = json.loads(resp['Payload'].read().decode())
            if payload['status'] != 0:
                raise dml.Error(payload['error'])
            return payload['result']
        waiter = dml.FnUpdater.from_waiter(waiter, lambda_update_fn)
        waiter.update()
        return waiter


@dataclass
class TmpRemote:
    name: str
    result: dml.Node|None = None


def id_fn(path):
    with open(path, 'rb') as f:
        return sha256(f.read()).hexdigest()


@dataclass
class S3:
    bucket: str
    prefix: str = ''
    session: boto3.Session = field(default_factory=boto3.Session)

    def __post_init__(self):
        self.prefix = self.prefix.strip('/')

    @property
    def client(self):
        return self.session.client('s3')

    def _put_bytes(self, obj: bytes) -> str:
        _id = sha256(obj).hexdigest()
        key = f'{self.prefix}/{_id}.bytes'
        self.client.put_object(Body=obj, Bucket=self.bucket, Key=key)
        return f's3://{self.bucket}/{key}'

    def put_bytes(self, dag: dml.Dag, obj: bytes) -> dml.Node:
        uri = self._put_bytes(obj)
        return dag.put(dml.Resource(uri))

    def get_bytes(self, resource: dml.Resource|dml.Node|str) -> bytes:
        if isinstance(resource, dml.Node):
            resource = resource.value()
            assert isinstance(resource, dml.Resource)
        if isinstance(resource, dml.Resource):
            resource = resource.uri
        parsed = urlparse(resource)
        obj = self.client.get_object(Bucket=parsed.netloc, Key=parsed.path[1:])
        return obj['Body'].read()

    def delete(self, *uris):
        parsed = defaultdict(list)
        for uri in uris:
            p = urlparse(uri)
            parsed[p.netloc].append(p.path[1:])
        for k, v in parsed.items():
            self.client.delete_objects(Bucket=k, Delete={'Objects': [{'Key': x} for x in v]})

    def list(self):
        paginator = self.client.get_paginator('list_objects_v2')
        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(Bucket=self.bucket, Prefix=f'{self.prefix}/')
        for page in page_iterator:
            for js in page['Contents']:
                yield f's3://{self.bucket}/{js["Key"]}'

    @contextmanager
    def tmpdir(self):
        prefix = f'{self.prefix}/tmp/{uuid4().hex}'
        s3 = S3(bucket=self.bucket, prefix=prefix, session=self.session)
        try:
            yield s3
        finally:
            s3.delete(*s3.list())

    @contextmanager
    def tmp_local(self, resource: dml.Resource|dml.Node|str):
        if isinstance(resource, dml.Node):
            resource = resource.value()
            assert isinstance(resource, dml.Resource)
        if isinstance(resource, dml.Resource):
            resource = resource.uri
        parsed = urlparse(resource)
        with TemporaryDirectory(prefix='dml-s3-') as tmpd:
            tmpf = f'{tmpd}/obj'
            self.client.download_file(parsed.netloc, parsed.path[1:], tmpf)
            yield tmpf

    @contextmanager
    def tmp_remote(self, dag, id_fn=id_fn, suffix='bytes'):
        with TemporaryDirectory(prefix='dml-s3-') as tmpd:
            tmpf = f'{tmpd}/obj'
            obj = TmpRemote(tmpf)
            yield obj
            to = f'{self.prefix}/{id_fn(tmpf)}.{suffix}'
            self.client.upload_file(tmpf, self.bucket, to)
        obj.result = dag.put(dml.Resource(f's3://{self.bucket}/{to}'))

    def write_parquet(self, dag, df, **kw) -> dml.Node:
        hsh = sha256()
        if pl is not None and isinstance(df, pl.DataFrame):
            for x in df.hash_rows().sort():
                hsh.update(x.to_bytes(64, "little"))
            hsh = hsh.hexdigest()
            with self.tmp_remote(dag, id_fn=lambda x: hsh, suffix='pl.parquet') as tmp:
                df.write_parquet(tmp.name, **kw)
            assert isinstance(tmp.result, dml.Node)
            return tmp.result
        elif pd is not None and isinstance(df, pd.DataFrame):
            for x in pd.util.hash_pandas_object(df):
                hsh.update(x.to_bytes(64, 'little'))
            hsh = hsh.hexdigest()
            with self.tmp_remote(dag, id_fn=lambda x: hsh, suffix='pd.parquet') as tmp:
                df.to_parquet(tmp.name, **kw)
            assert isinstance(tmp.result, dml.Node)
            return tmp.result
        msg = f'Unrecognized type for write_parquet: {type(df) = }'
        raise ValueError(msg)

    def tar(self, dag: dml.Dag, path: str|Path, filter_fn: Callable = lambda x: x) -> dml.Node:
        def hash_fn(path):
            with gzip.open(path, 'rb') as f:
                _hash = sha256(f.read()[8:]).hexdigest()
            return _hash
        with self.tmp_remote(dag, id_fn=hash_fn, suffix='tar.gz') as tmp:
            with tarfile.open(tmp.name, "w:gz") as tar:
                tar.add(path, arcname='/', filter=filter_fn)
        assert isinstance(tmp.result, dml.Node)
        return tmp.result

    def untar(self, tarball: dml.Node|dml.Resource, to: str|Path) -> None:
        with self.tmp_local(tarball) as tball:
            with tarfile.open(tball, 'r:gz') as tf:
                tf.extractall(to)

    def scriptify(self, dag: dml.Dag, fn: Callable) -> dml.Node:
        src = dedent(inspect.getsource(fn))
        with self.tmp_remote(dag, suffix='py') as tmpf:
            with open(tmpf.name, 'w') as f:
                f.write('#!/usr/bin/env python3')
                f.write(f'\n\n{src}\n')
                f.write(dedent(f'''
                if __name__ == '__main__':
                    import sys

                    import daggerml as dml

                    with dml.Api(initialize=True) as api:
                        with api.new_dag('execution', 'misc-message', dump=sys.stdin.read()) as dag:
                            {fn.__name__}(dag)
                        if dag.result is None:
                            dag.commit(dml.Error('dag finished without a result'))
                        print(api.dump(dag.result))
                    print('dml finished running', {fn.__name__!r}, file=sys.stderr)
                '''))
        assert isinstance(tmpf.result, dml.Node)
        return tmpf.result


@dataclass
class StreamReader:
    stream: Any
    prefix: str = 'misc'
    pattern: str|None = None
    store: List[str] = field(default_factory=list)

    async def read_stream(self):
        logger.info('starting to read stream: %r', self.prefix)
        while True:
            line = await self.stream.readline()
            if not line:
                break
            line = line.decode('utf-8').strip()
            logger.info('%s <==> %s', self.prefix, line)
            if self.pattern and re.search(self.pattern, line):
                logger.info('%s <==> storing previous line', self.prefix)
                self.store.append(re.search(self.pattern, line).groups()[0])


async def arun(*cmd, out=None, err=None):
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout = StreamReader(proc.stdout, 'STDOUT', out)
    stderr = StreamReader(proc.stderr, 'STDERR', err)
    stdout_task = asyncio.create_task(stdout.read_stream())
    stderr_task = asyncio.create_task(stderr.read_stream())
    await asyncio.gather(stdout_task, stderr_task)
    await proc.wait()
    return proc, stdout.store, stderr.store

@dataclass
class Dkr:
    session: boto3.Session = field(default=None)

    def __post_init__(self):
        self.session = self.session or boto3.Session()

    async def _abuild(self, tarball: dml.Resource, s3: S3, dockerfile: str|None = None):
        cmd = ['docker', 'build', '--platform=linux/amd64']
        if dockerfile:
            cmd.extend(['-f', dockerfile])
        if platform.system().lower() == 'darwin':
            cmd.append('--load')
        with TemporaryDirectory(prefix='dml-dkr-') as tmpd:
            s3.untar(tarball, tmpd)
            proc, _, err = await arun(*cmd, tmpd, err=r'^#[0-9]+ writing image sha256:([^\s]+)\s?(done)?$')
            if proc.returncode != 0:
                raise dml.Error('failed to build docker image')
        id, = sorted(set(err))
        if platform.system().lower() == 'darwin':
            # docker in osx is weird
            await arun('docker', 'images', '--no-trunc')
        return id

    def build(self, dag: dml.Dag, tarball: dml.Node, dockerfile: str|dml.Node|None, s3: S3) -> dml.FnWaiter:
        resource = dml.Resource('py-dkr:build')
        expr = [dag.put(resource), tarball, dag.put(dockerfile)]
        waiter = dag.start_fn(*expr)

        def update_fn(cache_key, dump, reqs):
            with dml.Api(initialize=True) as api:
                with api.new_dag('asdf', 'qwer', dump=dump) as fndag:
                    _, tball, dkrfile = fndag.expr.value()
                    id = asyncio.run(self._abuild(tball, s3, dockerfile=dkrfile))
                    rsrc = dml.Resource(f'dkr:{id}')
                    fndag.commit(fndag.put(rsrc))
                dump = api.dump(fndag.result)
            return dump
        return dml.FnUpdater.from_waiter(waiter, update_fn)

    def push(self, dag: dml.Dag, img: dml.Node, repo: dml.Node):
        expr = [dag.put(dml.Resource('py-dkr:push')), img, repo]
        waiter = dag.start_fn(*expr)
        if waiter.get_result() is not None:
            return waiter
        def docker_login(registry_url):
            cmd0 = ['aws', 'ecr', 'get-login-password', '--region', self.session.region_name]
            proc0 = subprocess.run(cmd0, capture_output=True)
            cmd1 = ["docker", "login", "-u", 'AWS', "--password-stdin", registry_url]
            proc1 = subprocess.run(cmd1, input=proc0.stdout, capture_output=True)
            if proc1.returncode != 0:
                msg = "Docker login failed. Error message: " + proc1.stderr.decode()
                logger.error(msg)
                raise dml.Error(msg)
        def update_fn(cache_key, dump, reqs):
            with dml.Api(initialize=True) as api:
                with api.new_dag('asdf', 'qwer', dump=dump) as fndag:
                    _, img, repo = fndag.expr
                    _repo = repo.value()
                    assert isinstance(_repo, dml.Resource)
                    docker_login(_repo.id)
                    img_id = img.value().id
                    new_uri = f'{_repo.id}:{img_id}'
                    subprocess.run(['docker', 'tag', img_id, new_uri], check=True)
                    subprocess.run(['docker', 'push', new_uri], check=True)
                    rsrc = dml.Resource(f'dkr:{new_uri}', requires=(_repo,))
                    fndag.commit(fndag.put(rsrc))
                dump = api.dump(fndag.result)
            return dump
        return dml.FnUpdater.from_waiter(waiter, update_fn)

    def make_fn(self, dag: dml.Dag, image: dml.Node, script: dml.Node) -> dml.FnWaiter:
        expr = [dag.put(dml.Resource('py-dkr:make_fn')), image, script]
        waiter = dag.start_fn(*expr)
        if waiter.get_result() is not None:
            return waiter
        def update_fn(cache_key, dump, reqs):
            with dml.Api(initialize=True) as api:
                with api.new_dag('asdf', 'qwer', dump=dump) as fndag:
                    _, _img, _script = fndag.expr
                    rsrc = dml.Resource('py-dkr:run', requires=(_img.value(), _script.value()))
                    fndag.commit(fndag.put(rsrc))
                dump = api.dump(fndag.result)
            return dump
        return dml.FnUpdater.from_waiter(waiter, update_fn)

    async def arun(self, dag: dml.Dag, fn: dml.Node, *args, s3: S3) -> dml.FnWaiter:
        expr = [fn, *[dag.put(x) for x in args]]
        waiter = dag.start_fn(*expr)
        if waiter.get_result() is not None:
            return waiter
        rsrc = fn.value()
        assert isinstance(rsrc, dml.Resource)
        img_id = rsrc.requires[0].id
        script = rsrc.requires[1].uri
        with s3.tmpdir() as subs3:
            dump_uri = subs3._put_bytes(waiter.dump.encode())
            resp_uri = f's3://{subs3.bucket}/{subs3.prefix}/result.dump'
            cmd = ['docker', 'run', '--platform=linux/amd64', '--rm']
            a = self.session.get_credentials()
            if a.access_key is not None:
                cmd.extend(['-e', f'AWS_ACCESS_KEY_ID={a.access_key}'])
            if a.secret_key is not None:
                cmd.extend(['-e', f'AWS_SECRET_ACCESS_KEY={a.secret_key}'])
            if a.token is not None:
                cmd.extend(['-e', f'AWS_SESSION_TOKEN={a.token}'])
            cmd = [
                *cmd,
                '-e', f"DML_SCRIPT_URI={script}",
                '-e', f"DML_DUMP_URI={dump_uri}",
                '-e', f"DML_RESULT_URI={resp_uri}",
                img_id,
            ]
            logger.info('submitting cmd: %r', cmd)
            proc, *_ = await arun(*cmd)
            if proc.returncode != 0:
                raise RuntimeError('failed to run docker image')
            dump = subs3.get_bytes(resp_uri)
        dag.api.load(dump)
        return waiter

    def run(self, dag: dml.Dag, fn: dml.Node, *args, s3: S3) -> dml.FnWaiter:
        return asyncio.run(self.arun(dag, fn, *args, s3=s3))
