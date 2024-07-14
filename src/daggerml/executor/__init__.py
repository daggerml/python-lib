#!/usr/bin/env python3
"""
* In general, methods ending with _ denote functions that take non Node arguments
  (and hence require compute). For example, building a tarball requires us to
  actually build the tarball before we can know the ID.
"""
import gzip
import inspect
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import tarfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from hashlib import sha512
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from textwrap import dedent
from time import time
from typing import Any, Callable, Dict, Optional, Type
from uuid import uuid4

import daggerml as dml

logger = logging.getLogger(__name__)
dml_root = os.path.dirname(dml.__file__)
CACHE_LOC = str(Path.home() / '.local/cache/dml')
CACHE_LOC = Path(os.environ.get('DML_CACHE_LOC', CACHE_LOC))
HOSTNAME = socket.getfqdn()

def _sh_cmd_hatch(env):
    def inner(x):
        return ['hatch', '-e', env, 'run', x]
    return inner

def _sh_cmd_python(executable):
    def inner(x):
        return [executable, x]
    return inner

SH_CMDS = {'hatch': _sh_cmd_hatch, 'python': _sh_cmd_python}


@contextmanager
def cd(path):
   old_path = os.getcwd()
   os.chdir(path)
   try:
       yield
   finally:
       os.chdir(old_path)


def make_tarball(source_dir, output_filename, filter_fn=lambda x: x):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname='/', filter=filter_fn)
    with gzip.open(output_filename, 'rb') as f:
        _hash = sha512(f.read()[8:]).hexdigest()
    return _hash


def extract_tarball(src, dest):
    with tarfile.open(src, 'r:gz') as tf:
        tf.extractall(dest)


@dataclass
class Cache:
    root: Path = CACHE_LOC

    def _resolve(self, obj: dml.Resource):
        return (self.root/obj.id)

    def exists(self, obj: dml.Resource):
        return self._resolve(obj).is_file()

    def put(self, src_path: str|Path|Type["Cache"], resource):
        fpath = self.root / resource.id
        if isinstance(src_path, Cache):
            src_path = src_path._resolve(resource)
        assert isinstance(src_path, (str, Path))
        if not (fpath).exists():
            os.makedirs(fpath.parent, exist_ok=True)
            shutil.copy(src_path, fpath)
            return True
        return False

    def copy(self, resource: dml.Resource, dst_path: Path|str):
        fpath = self.root / resource.id
        assert not Path(dst_path).exists(), f'destination path {str(dst_path)} already exists'
        shutil.copy(fpath, dst_path)

    def cat(self, obj: dml.Resource):
        with open(self._resolve(obj), 'rb') as f:
            return f.read()


@dataclass
class Sh:
    dag: dml.Dag
    cache: Cache = field(default_factory=Cache)
    name = f'daggerml/host:{HOSTNAME}/sh'

    @property
    def rsrc(self):
        return dml.Resource(self.name)

    def resource(self, _id):
        return dml.Resource(f'{self.rsrc.id}/{_id}')

    def tar_(self, src, filter_fn=lambda x: x):
        tmpf = NamedTemporaryFile(prefix='dml-', delete=False)
        try:
            _hash = make_tarball(src, tmpf.name, filter_fn=filter_fn)
            resource = self.resource(f'{_hash}.tar.gz')
            self.cache.put(tmpf.name, resource)
            return self.dag.put(resource)
        finally:
            if os.path.exists(tmpf.name):
                os.unlink(tmpf.name)

    def put_file_(self, src):
        with open(src, 'rb') as f:
            _hash = sha512(f.read()).hexdigest()
        rsrc = self.resource(f'{_hash}.file')
        self.cache.put(src, rsrc)
        return self.dag.put(rsrc)

    def put_bytes_(self, bts):
        with NamedTemporaryFile(prefix='dml-bytes-', mode='wb+') as f:
            f.write(bts)
            f.flush()
            f.seek(0)
            return self.put_file_(f.name)

    def get_bytes(self, node: dml.Node):
        resource = node.value()
        assert isinstance(resource, dml.Resource)
        assert resource.id.startswith(self.name + '/')
        return self.cache.cat(resource)

    @contextmanager
    def hydrate(self, **rsrcs: dml.Node):
        with TemporaryDirectory(prefix='dml-hydrate-') as tmpd:
            for k, v in rsrcs.items():
                to = f'{tmpd}/{k}'
                if isinstance(v, dml.Node):
                    v = v.value()
                if isinstance(v, dml.Resource):
                    self.cache.copy(v, to)
                else:
                    raise TypeError(f'invalid type: ({k} = {type(v)})')
            yield tmpd

    def run_script(
        self,
        exec_name: str,
        script: dml.Node,
        mounts: Dict[str, dml.Node],
        *args: dml.Node,
        use_cache: bool = False,
        **exec_kw
    ):
        # TODO: create new repo and set env vars
        exec_fn = SH_CMDS[exec_name](**exec_kw)
        resource = self.resource(f'script/{exec_name}')
        fnwaiter = self.dag.start_fn(resource, mounts, *args, use_cache=use_cache)
        res = fnwaiter.get_result()
        if res is not None:
            return res
        with dml.Api(initialize=True) as api:
            fndag = dml.new('exec-dag', 'executing dag', dump=fnwaiter.dump, api_flags=api.flags)
            config_dir = api.flags["config-dir"]
            for k, v in {'script': script, **mounts}.items():
                self.cache.copy(v.value(), f'{config_dir}/{k}')
            env = os.environ.copy()
            env['DML_INPUT_DUMP'] = f'{config_dir}/dag.json'
            env['DML_OUTPUT_DUMP'] = f'{config_dir}/result.json'
            with open(env['DML_INPUT_DUMP'], 'w') as f:
                json.dump({'tok': fndag.tok, 'flags': api.flags}, f)
            subprocess.run(['chmod', '+x', f'{config_dir}/script'], check=True)
            resp = subprocess.run(exec_fn(f'{config_dir}/script'), env=env, stderr=subprocess.PIPE)
            if resp.returncode != 0:
                stderr = resp.stderr.decode()
                raise dml.Error('subprocess failed', context={'stderr': stderr})
            with open(env['DML_OUTPUT_DUMP'], 'r') as f:
                self.dag.load_ref(f.read())
        return fnwaiter.get_result()

    def scriptify(
        self,
        fn: Callable[[Any], Any],
    ):
        source_code = dedent(inspect.getsource(fn))
        with NamedTemporaryFile(prefix='dml-', suffix='.py') as tmpf:
            with open(tmpf.name, 'w') as f:
                f.write('#!/usr/bin/env python3')
                f.write(f'\n\n{source_code}\n')
                f.write(dedent(f'''
                if __name__ == '__main__':
                    import json
                    import os

                    import daggerml as dml

                    with open(os.environ['DML_INPUT_DUMP']) as f:
                        js = json.load(f)
                    with dml.Dag(js['tok'], dml.Api(flags=js['flags'])) as dag:
                        result = {fn.__name__}(dag)
                    import daggerml as dml
                    with open(os.environ['DML_OUTPUT_DUMP'], 'w') as f:
                        f.write(dag.dump(result))
                    print('dml finished running', {fn.__name__!r})
                '''))
            script = self.put_file_(tmpf.name)
            return script

    def run_py(self,
               fn: Callable[[Any], Any]|dml.Node,
               *args: dml.Node,
               mounts: Optional[Dict[str, dml.Node]] = None,
               executable: str = sys.executable,
               use_cache: bool = False):
        if not isinstance(fn, dml.Node):
            fn = self.scriptify(fn)
        mounts = mounts or {}
        result = self.run_script('python', fn, mounts, *args, use_cache=use_cache, executable=executable)
        return result

    def run_hatch(self,
                  fn: Callable[[Any], Any]|dml.Node,
                  *args: dml.Node,
                  mounts: Optional[Dict[str, dml.Node]] = None,
                  env: str = 'default',
                  use_cache: bool = False):
        # script = self.scriptify(fn)
        if not isinstance(fn, dml.Node):
            fn = self.scriptify(fn)
        mounts = mounts or {}
        result = self.run_script('hatch', fn, mounts, *args, use_cache=use_cache, env=env)
        return result


def docker_build(path, dkr_path=None, tag=None, flags=()):
    if True:
        return
    img = 'dml'
    tag = tag or f'id_{int(time())}_{uuid4().hex}'
    cmd = ['docker', 'build', '-t', f'{img}:{tag}']
    if dkr_path is not None:
        cmd.extend(['-f', dkr_path])
    with cd(path):
        proc = subprocess.run([*cmd, *flags, '.'])
    if proc.returncode != 0:
        raise RuntimeError(f'docker build exited with code: {proc.returncode!r}')
    return {
        'repository': img,
        'hash': tag,
        'id': f'{img}:{tag}',
    }

def docker_run(dkr: dict, script: str, **mounts: str):
    """
    run a script in a docker image (potentially with other named mounts)

    Parameters
    ==========
    dkr: Dict[str, str]
        assumes `dkr['id']` is populated the docker local image
    script: str
        the local path to the script to run. This will be mounted at the same
        path in the docker image. The script should be executable and should
        take one argument (where to write the result to)
    **mounts: str
        for association k, v: we mount local `k` to `v` on the image.
        All of these mounts are read only.

    Returns
    =======
    pass
    """
    with NamedTemporaryFile() as tmpf:
        cmd = [
            'docker', 'run',
            f'--mount=type=bind,source={script},target={script}',
            f'--mount=type=bind,source={tmpf.name},target={tmpf.name}',
        ]
        for k, v in mounts.items():
            cmd.append(
                f'--mount=type=bind,source={k},target={v},readonly'
            )
        proc = subprocess.run([*cmd, dkr['id'], script, tmpf.name], capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f'{proc.returncode}:\n{proc.stderr}')
        with open(tmpf.name, 'r') as f:
            return dml.from_json(f.read())

@dataclass
class Dkr:
    dag: dml.Dag
    name = f'daggerml/host:{HOSTNAME}/docker'

    def resource(self, **kw):
        return dml.Resource.from_dict({'exec': self.name, **kw})

    def build(self, tarball: dml.Node, dkr_path: Optional[str|dml.Node] = None, *,
              cache: bool = True):
        """
        build a docker image

        Notes
        -----
        * This does not check to ensure the docker image exists locally.
          We need to implement `cache='replace'` as porcelain.
        """
        if isinstance(dkr_path, dml.Node):
            ar_path = self.dag.get_value(dkr_path)
        else:
            ar_path = dkr_path
            dkr_path = self.dag.put(ar_path)
        fndag = self.dag.start_fn(self.resource(type='build'), tarball, dkr_path, cache=cache)
        if isinstance(fndag, dml.Node):
            return fndag
        with self.dag.sh.hydrate(tarball=tarball) as tb_path:
            resp = docker_build(f'{tb_path}/tarball', dkr_path=ar_path)
        print(resp)
        rsrc = self.resource(type='image', **resp)
        return fndag.commit(rsrc)

    def exists(self, img: dml.Node):
        cmd = ['docker', 'image', 'inspect', self.dag.get_value(img).info['id']]
        print('command:', ' '.join(cmd))
        resp = subprocess.run(cmd, check=False)
        return resp.returncode == 0

    def run(self, img: dml.Node, fn: Callable[[Any], Any], *args: dml.Node,
            mounts: Optional[Dict[str, dml.Node]] = None,
            cache: bool = True):
        fndag = self.dag.start_fn(img, dedent(inspect.getsource(fn)), args, mounts, cache=cache)
        if isinstance(fndag, dml.Node):
            return fndag
        assert isinstance(fndag, dml.FnDag)  # just to quiet pylint
        with fndag:
            assert isinstance(fndag, dml.FnDag)  # just to quiet pylint
            # `mounts` and `img` redefinition below assures all will be py-types
            img, src, _args, mounts = fndag.expr
            if mounts is None:
                mounts = {}
            assert isinstance(img, dml.Resource)
            with self.dag.sh.hydrate(**mounts) as d0:
                mnt_dict = {f'{d0}/{k}': f'/opt/daggerml/{k}' for k in mounts}
                with TemporaryDirectory(prefix='dml-') as d1:
                    with open(f'{d1}/script.py', 'w') as f:
                        f.write('#!/usr/bin/env python3')
                        f.write(f'\n\n{src}\n')
                        f.write(dedent(f'''
                        if __name__ == '__main__':
                            try:
                                result = {fn.__name__}(*{_args!r})
                            except KeyboardInterrupt:
                                raise
                            except Exception as e:
                                import daggerml as dml
                                result = dml.Error.from_ex(e)
                            import sys
                            import daggerml as dml
                            with open(sys.argv[1], 'w') as f:
                                f.write(dml.to_json(result))
                            print('dml finished running', {fn.__name__!r})
                        '''))
                    subprocess.run(['chmod', '+x', f'{d1}/script.py'], check=True)
                    mnt_dict[d1] = d1
                    result = docker_run(img.info, f'{d1}/script.py', **mnt_dict)
            return fndag.commit(result)
