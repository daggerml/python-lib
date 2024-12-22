import asyncio
import fcntl
import gzip
import inspect
import json
import logging
import platform
import re
import stat
import subprocess
import tarfile
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from textwrap import dedent
from typing import Any, Callable, List
from urllib.parse import urlparse
from uuid import uuid4

import boto3  # TODO: Refactor so that these are optional

import daggerml as dml
from daggerml.core import js_dumps

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
CACHE_LOC = Path.home() / '.local/dml/cache'


def session_to_env(session: boto3.Session):
    a = session.get_credentials()
    out = {
        "AWS_ACCESS_KEY_ID": a.access_key,
        "AWS_SECRET_ACCESS_KEY": a.secret_key,
        "AWS_SESSION_TOKEN": a.token,
        "AWS_REGION": session.region_name,
        "AWS_DEFAULT_REGION": session.region_name,
    }
    out = {k: v for k, v in out.items() if v is not None}
    return out


@dataclass
class TmpRemote:
    name: str
    result: dml.Node|None = None


def id_fn(path):
    with open(path, 'rb') as f:
        return sha256(f.read()).hexdigest()

def scriptify(fn: Callable) -> str:
    src = dedent(inspect.getsource(fn))
    txt = [
        "#!/usr/bin/env python3",
        f"\n\n{src}\n",
        dedent(f"""
        if __name__ == '__main__':
            import contextlib
            import sys

            import daggerml as dml

            with dml.Api(initialize=True) as api:
                with contextlib.redirect_stdout(sys.stderr):
                    dump = sys.stdin.read()
                    print('found dump:', dump)
                    with api.new_dag('execution', 'misc-message', dump=dump) as dag:
                        print('Starting dag...')
                        {fn.__name__}(dag)
                    if dag.result is None:
                        dag.commit(dml.Error('dag finished without a result'))
                    print('dml finished running', {fn.__name__!r})
                print(api.dump(dag.result))
        """).strip(),
    ]
    return '\n'.join(txt)

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
            for x in pandas.util.hash_pandas_object(df):
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
        with self.tmp_remote(dag, suffix='py') as tmpf:
            with open(tmpf.name, 'w') as f:
                f.write(scriptify(fn))
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


async def _adkr_build(path, flags):
    cmd = ['docker', 'build', *flags]
    proc, _, err = await arun(*cmd, path, err=r'^#[0-9]+ writing image sha256:([^\s]+)\s?(done)?$')
    if proc.returncode != 0:
        raise dml.Error('failed to build docker image')
    id, = sorted(set(err))
    if platform.system().lower() == 'darwin':
        # docker in osx is weird
        await arun('docker', 'images', '--no-trunc')
    return id

def _dkr_build(path, flags):
    return asyncio.run(_adkr_build(path, flags))

@dataclass
class Dkr:
    session: boto3.Session = field(default_factory=boto3.Session)
    scheme = 'py-dkr'

    def build(self, dag, tarball, flags, s3):
        resource = dml.Resource(f'{self.scheme}:build')
        flags = flags or []  # in case it's None
        waiter = dag.start_fn(resource, tarball, flags)

        def update_fn(cache_key, dump, data):
            with dml.Api(initialize=True) as api:
                with api.new_dag('asdf', 'qwer', dump=dump) as fndag:
                    _, tball, flags = fndag.expr.value()
                    with TemporaryDirectory(prefix='dml-dkr-') as tmpd:
                        s3.untar(tball, tmpd)
                        id = _dkr_build(tmpd, flags)
                    fndag.commit(fndag.put(dml.Resource(f'{self.scheme}:{id}')))
                dump = api.dump(fndag.result)
            return dump
        return dml.FnUpdater.from_waiter(waiter, update_fn)

    def make_fn(self, dag, image, script, flags):
        waiter = dag.start_fn(dml.Resource(f'{self.scheme}:make_fn'), image, script, flags)
        def update_fn(cache_key, dump, data):
            with dml.Api(initialize=True) as api:
                with api.new_dag('asdf', 'qwer', dump=dump) as fndag:
                    _, _img, _script, _flags = fndag.expr.value()
                    _data = js_dumps([_img.id, _script.uri, _flags])
                    fndag.commit(fndag.put(dml.Resource(f'{self.scheme}:run', data=_data)))
                dump = api.dump(fndag.result)
            return dump
        return dml.FnUpdater.from_waiter(waiter, update_fn)

    def run(self, dag, fn, *args, s3):
        waiter = dag.start_fn(fn, *args)
        def update_fn(cache_key, dump, data):
            rsrc = fn.value()
            assert isinstance(rsrc, dml.Resource)
            img_id, script, flags = rsrc.js
            with s3.tmpdir() as subs3:
                dump_uri = subs3._put_bytes(waiter.dump.encode())
                resp_uri = f's3://{subs3.bucket}/{subs3.prefix}/result.dump'
                cmd = ['docker', 'run', '--rm', *flags]
                for k, v in session_to_env(self.session).items():
                    cmd.extend(['-e', f'{k}={v}'])
                cmd.extend([
                    '-e', f"DML_SCRIPT_URI={script}",
                    '-e', f"DML_DUMP_URI={dump_uri}",
                    '-e', f"DML_RESULT_URI={resp_uri}",
                    img_id,
                ])
                logger.info('submitting cmd: %r', cmd)
                proc, *_ = asyncio.run(arun(*cmd))
                if proc.returncode != 0:
                    raise RuntimeError('failed to run docker image')
                dump = subs3.get_bytes(resp_uri)
                return dump
        return dml.FnUpdater.from_waiter(waiter, update_fn)


@dataclass
class Ecr:
    repo: str
    session: boto3.Session = field(default_factory=boto3.Session)
    scheme = 'py-ecr'

    @property
    def repo_uri(self):
        return self.repo if isinstance(self.repo, str) else self.repo.value().id

    def login(self):
        cmd0 = ['aws', 'ecr', 'get-login-password', '--region', self.session.region_name]
        proc0 = subprocess.run(cmd0, capture_output=True)
        cmd1 = ["docker", "login", "-u", 'AWS', "--password-stdin", self.repo_uri]
        proc1 = subprocess.run(cmd1, input=proc0.stdout, capture_output=True)
        if proc1.returncode != 0:
            msg = "Docker login failed. Error message: " + proc1.stderr.decode()
            logger.error(msg)
            raise dml.Error(msg)

    def _push(self, img_id):
        self.login()
        new_uri = f'{self.repo_uri}:{img_id}'
        subprocess.run(['docker', 'tag', img_id, new_uri], check=True)
        subprocess.run(['docker', 'push', new_uri], check=True)
        return new_uri

    def push(self, dag, img):
        waiter = dag.start_fn(dag.put(dml.Resource(f'{self.scheme}:push')), img, self.repo)
        if waiter.get_result() is not None:
            return waiter
        def update_fn(cache_key, dump, data):
            with dml.Api(initialize=True) as api:
                with api.new_dag('asdf', 'qwer', dump=dump) as fndag:
                    _, img = fndag.expr.value()
                    fndag.commit(fndag.put(dml.Resource(self._push(img.id))))
                dump = api.dump(fndag.result)
            return dump
        return dml.FnUpdater.from_waiter(waiter, update_fn)


@dataclass
class Lambda:
    session: boto3.Session = field(default_factory=boto3.Session)
    scheme = 'py-dkr-lambda'

    def make_fn(self, dag, lam, *args):
        rsrc = dml.Resource(f'{self.scheme}:make_fn')
        def update_fn(cache_key, dump, data):
            with dml.Api(initialize=True) as api:
                with api.new_dag('asdf', 'qwer', dump=dump) as fndag:
                    _, _lam, *_data = fndag.expr.value()
                    _data = js_dumps([x.uri for x in _data])
                    rsrc = dml.Resource(_lam.uri, data=_data)
                    fndag.commit(fndag.put(rsrc))
                dump = api.dump(fndag.result)
            return dump
        return dml.FnUpdater.from_waiter(dag.start_fn(rsrc, lam, *args), update_fn)

    def run(self, dag, fn, *args) -> dml.FnUpdater:
        def update_fn(cache_key, dump, data):
            rsrc = fn.value()
            logger.info('calling lambda %r', rsrc.uri)
            assert isinstance(rsrc, dml.Resource)
            resp = self.session.client('lambda').invoke(
                FunctionName=rsrc.uri,
                Payload=js_dumps({'cache_key': cache_key, 'data': data, 'dump': dump}).encode()
            )
            payload = json.loads(resp['Payload'].read().decode())
            if payload['status'] != 0:
                raise dml.Error(payload['error'])
            return payload['result']
        return dml.FnUpdater.from_waiter(dag.start_fn(fn, *args), update_fn)


@dataclass
class Cache:
    path: Path
    lockfile: Path = field(init=False)

    def __post_init__(self):
        if not isinstance(self.path, Path):
            self.path = Path(self.path)
        self.lockfile = Path(f'{self.path}.lock')
        self.lockfile.touch()

    @contextmanager
    def lock(self):
        with open(self.lockfile, 'w') as lf:
            try:
                fcntl.lockf(lf, fcntl.LOCK_EX)
                yield
            finally:
                fcntl.lockf(lf, fcntl.LOCK_UN)

    @contextmanager
    def open(self, *x, **kw):
        with open(self.path, *x, **kw) as f:
            yield f

    def exists(self):
        return self.path.exists()

    def put(self, data):
        with self.open('w') as f:
            f.write(js_dumps(data))

    def get(self):
        if not self.path.exists():
            return
        with self.open() as f:
            return json.load(f)

    def delete(self):
        self.path.unlink()
        self.lockfile.unlink()

    def __enter__(self):
        yield self

    def __exit__(self, *errs, **kw):
        self.delete()

@dataclass
class Ssh:
    host: str
    port: int|None = None
    user: str|None = None
    pkey: str|None = None
    opts: list = field(default_factory=list)

    def execute_ssh_command(self, command, input=None, timeout=60):
        """
        Executes a command on a remote host via SSH using the system's ssh command.

        Args:
            command (str): The command to execute on the remote host.
            timeout (int, optional): Timeout for the SSH command in seconds. Default is 10.

        Returns:
            tuple: A tuple containing (stdout, stderr, exit_code) from the command execution.
        """
        ssh_command = [
            "ssh",
            f"{self.user}@{self.host}" if self.user is not None else self.host,
        ]
        if self.port is not None:
            ssh_command.extend(["-p", str(self.port)])
        if self.pkey is not None:
            ssh_command.extend(["-i", self.pkey])
        ssh_command.extend(self.opts)
        ssh_command.append(command)
        try:
            process = subprocess.run(
                ssh_command,
                input=f"{input}\n",
                text=True,
                capture_output=True,
                timeout=timeout,
            )
            return process.stdout, process.stderr, process.returncode
        except subprocess.TimeoutExpired as e:
            return "", f"Command timed out after {timeout} seconds: {e}", 2
        except Exception as e:
            return "", str(e), 1

    def run(self, cmd, input=None):
        stdout, stderr, return_code = self.execute_ssh_command(cmd, input=input)
        if return_code != 0:
            stderr = stderr.strip()
            logger.error("remote execution failed with stderr: %s", stderr)
            raise dml.Error("remote ssh command failed",
                            {"returncode": return_code, "stderr": stderr},
                            "failed_remote_execution")
        return stdout.strip()

def run_local_cmd(cmd, input):
    proc = subprocess.run(cmd, input=input, capture_output=True, text=True)
    if proc.returncode != 0:
        raise dml.Error('local command failed',
                        {"returncode": proc.returncode, "stderr": proc.stderr.strip()},
                        'failed_local_execution')
    return proc.stdout.strip()

@dataclass
class Local:
    scheme = 'py-local'

    def make_fn(self, dag, fn, flavor, env, conn_params=None, preambles=None):
        data = {
            "source": scriptify(fn),
            "flavor": flavor,
            "env": env,
            "preambles": preambles or []
        }
        if conn_params is not None:
            data["conn_params"] = conn_params
        return dag.put(dml.Resource(f"{self.scheme}:run", data=js_dumps(data)))

    @staticmethod
    def _flavor_to_cmd(flavor, env, script_loc):
        if flavor == "conda":
            return ["conda", "run", "--no-capture-output", "-n", env, script_loc]
        elif flavor == "hatch":
            return ["hatch", "-e", env, "run", script_loc]
        else:
            msg = f"unrecognized python flavor: {flavor}"
            raise ValueError(msg)

    def _submit_local(self, source, flavor, env, preambles, dump):
        with NamedTemporaryFile(dir="/tmp/", suffix=".py") as tmpf:
            tmpf.write(source.encode())
            tmpf.seek(0)
            script = Path(tmpf.name)
            script.chmod(script.stat().st_mode | stat.S_IEXEC)  # chmod +x
            cmd = [*preambles, *self._flavor_to_cmd(flavor, env, str(script))]
            return run_local_cmd(cmd, dump)

    def _submit_remote(self, source, flavor, env, preambles, dump, conn_params):
        script_loc = f"/tmp/dml-{uuid4().hex[:8]}.py"
        cmd = [*preambles, *self._flavor_to_cmd(flavor, env, script_loc)]
        ssh = Ssh(**conn_params)
        ssh.run(f'cat > {script_loc}', input=source)
        try:
            ssh.run(f'chmod +x {script_loc}')
            resp = ssh.run(" ".join(cmd), dump)
            return resp
        finally:
            ssh.run(f"rm {script_loc} || echo Failed to delete {script_loc!r}")

    def run(self, dag, fn_rsrc, *args) -> dml.FnUpdater:
        def update_fn(cache_key, dump, data):
            # TODO: This should do the double-fork trick to detach this process
            # and write the PID to disk using cache_key. The proc can then check
            # it and work async.
            data = json.loads(data)
            if "conn_params" in data:
                return self._submit_remote(dump=dump, **data)
            return self._submit_local(dump=dump, **data)
        return dml.FnUpdater.from_waiter(dag.start_fn(fn_rsrc, *args), update_fn)
