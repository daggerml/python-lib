#!/usr/bin/env python3
"""
* In general, methods ending with _ denote functions that take non Node arguments
  (and hence require compute). For example, building a tarball requires us to
  actually build the tarball before we can know the ID.
"""
import gzip
import inspect
import logging
import os
import shutil
import subprocess
import sys
import tarfile
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha512
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from textwrap import dedent
from time import time
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

import daggerml as dml
from daggerml.core import CACHE_LOC, Resource

logger = logging.getLogger(__name__)
dml_root = os.path.dirname(dml.__file__)


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
class Sh:
    dag: dml.Dag
    name = 'sh'

    def resource(self, **kw):
        return Resource.from_dict({'exec': self.name, **kw})

    def _resolve(self, obj: Resource|dml.Node):
        if isinstance(obj, dml.Node):
            obj = self.dag.get_value(obj)
        return (CACHE_LOC/obj.info['id'])

    @staticmethod
    def _put_obj(src_path, file_id):
        fpath = CACHE_LOC/file_id
        if not (fpath).exists():
            os.rename(src_path, fpath)
            return True
        return False

    @staticmethod
    def _get_obj(file_id, dst_path):
        fpath = CACHE_LOC/file_id
        if not Path(dst_path).exists():
            shutil.copy(fpath, dst_path)

    def tar_(self, src, filter_fn=lambda x: x):
        tmpf = NamedTemporaryFile(prefix='dml-', delete=False)
        try:
            _hash = make_tarball(src, tmpf.name, filter_fn=filter_fn)
            file_id = f'{_hash}.tar.gz'
            self._put_obj(tmpf.name, file_id)
        finally:
            if os.path.exists(tmpf.name):
                os.unlink(tmpf.name)
        fndag = self.dag.start_fn(self.resource(), 'local', file_id)
        if isinstance(fndag, dml.Node):
            logger.info('returning cached value')
            return fndag
        logger.info('committing result')
        return fndag.commit(self.resource(id=file_id, type='tar'))

    def put_file_(self, src):
        with open(src, 'rb') as f:
            _hash = sha512(f.read()).hexdigest()
        rsrc = self.resource(id=f'{_hash}.file', type='file')
        shutil.copy(src, self._resolve(rsrc))
        return self.dag.put(rsrc)

    def put_bytes_(self, bts):
        with NamedTemporaryFile(prefix='dml-bytes-', mode='wb+') as f:
            f.write(bts)
            f.flush()
            f.seek(0)
            return self.put_file_(f.name)

    def store(self, x, **kw):
        if isinstance(x, dml.Node):
            return x
        if os.path.isdir(x):
            return self.tar_(x, **kw)
        if os.path.isfile(x):
            return self.put_file_(x, **kw)
        # not a file nor directory
        if isinstance(x, str):
            return self.dag.put(x, **kw)
        raise ValueError(f'invalid type: {type(x)}')

    @contextmanager
    def hydrate(self, **rsrcs: dml.Node):
        with TemporaryDirectory(prefix='dml-hydrate-') as tmpd:
            for k, v in rsrcs.items():
                to = f'{tmpd}/{k}'
                if isinstance(v, dml.Node):
                    v = self.dag.get_value(v)
                if isinstance(v, dml.Resource):
                    if v.info['type'] == 'tar':
                        extract_tarball(self._resolve(v), to)
                    elif v.info['type'] in {'bytes', 'file'}:
                        shutil.copy(self._resolve(v), to)
                    else:
                        raise ValueError(f'invalid type: {k} = {type(v)}')
                elif isinstance(v, str):
                    with open(to, 'w') as f:
                        f.write(v)
                else:
                    raise TypeError(f'invalid type: ({k} = {type(v)})')
            yield tmpd

    def exists(self, obj: dml.Node):
        obj = self.dag.get_value(obj)
        return self._resolve(obj).is_file()

    def run_fn_local(
        self,
        exec_fn: Callable[..., List[str]],
        resource_info: dict,
        fn: Callable[[Any], Any],
        *args: dml.Node,
        cache: bool = False
    ):
        source_code = dedent(inspect.getsource(fn))
        resource = dml.Resource.from_dict({'__source__': source_code, **resource_info})
        fndag = self.dag.start_fn(resource, *args, cache=cache)
        if isinstance(fndag, dml.Node):
            return fndag
        assert isinstance(fndag, dml.FnDag)  # just to quiet pylint
        with TemporaryDirectory(prefix='dml-') as tmpd:
            with open(f'{tmpd}/dag.json', 'w') as f:
                f.write(fndag.to_json())
            with open(f'{tmpd}/script.py', 'w') as f:
                f.write('#!/usr/bin/env python3')
                f.write(f'\n\n{source_code}\n')
                f.write(dedent(f'''
                if __name__ == '__main__':
                    import daggerml as dml
                    with open('{tmpd}/dag.json') as f:
                        fndag = dml.FnDag.from_json(f.read())
                    try:
                        result = {fn.__name__}(fndag)
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        import daggerml as dml
                        result = fndag.commit(dml.Error.from_ex(e))
                    import daggerml as dml
                    with open('{tmpd}/result.json', 'w') as f:
                        f.write(result.ref.to)
                    print('dml finished running', {fn.__name__!r})
                '''))
            subprocess.run(['chmod', '+x', f'{tmpd}/script.py'], check=True)
            subprocess.run(exec_fn(f'{tmpd}/script.py'), check=True)
            with open(f'{tmpd}/result.json', 'r') as f:
                result = dml.Node(dml.Ref(f.read()))
        return result

    def run_py(self,
               fn: Callable[[Any], Any],
               *args: dml.Node,
               executable: str = sys.executable,
               cache: bool = False):
        return self.run_fn_local(
            lambda x: [executable, x],
            dict(type='py-fn', id=executable),
            fn, *args, cache=cache)

    def run_hatch(self,
                  fn: Callable[[Any], Any],
                  *args: dml.Node,
                  env: str = 'default',
                  cache: bool = False):
        return self.run_fn_local(
            lambda x: ['hatch', '-e', env, 'run', x],
            dict(type='hatch-fn', id=env),
            fn, *args, cache=cache)


dml.Dag.register_ns(Sh)


def docker_build(path, dkr_path=None, tag=None, flags=()):
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
    name = 'dkr'

    def resource(self, **kw):
        return Resource.from_dict({'exec': self.name, **kw})

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
            assert isinstance(img, Resource)
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


dml.Dag.register_ns(Dkr)
