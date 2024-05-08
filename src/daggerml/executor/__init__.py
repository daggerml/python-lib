#!/usr/bin/env python3
import gzip
import json
import logging
import os
import subprocess
import tarfile
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha512
from tempfile import NamedTemporaryFile, TemporaryDirectory
from time import time
from typing import Optional
from urllib.parse import urlparse
from uuid import uuid4

import boto3

import daggerml as dml
from daggerml.core import CACHE_LOC

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


def make_tarball(source_dir, output_filename):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname='/')
    with gzip.open(output_filename, 'rb') as f:
        _hash = sha512(f.read()[8:]).hexdigest()
    return _hash
    # js = {'exec': 'local', 'id': _hash, 'path': output_filename}
    # return dml.Resource.from_dict(js)


def extract_tarball(src, dest):
    with tarfile.open(src, 'r:gz') as tf:
        tf.extractall(dest)


@contextmanager
def tmp_untar(tarball):
    with TemporaryDirectory(prefix='dml-') as tmpd:
        extract_tarball(tarball, tmpd)
        yield tmpd


def _s3_upload(src, s3_uri, boto_session=boto3, **kw):
    s3 = boto_session.client('s3')
    uri = urlparse(s3_uri)
    if uri.scheme != 's3':
        msg = f'dest ({s3_uri!r}) is not a valid s3 uri'
        raise ValueError(msg)
    bucket, key = uri.netloc, uri.path[1:]
    s3.upload_file(src, bucket, key)
    js = {**kw, 'exec': 's3', 'id': s3_uri, 'uri': s3_uri, 'bucket': bucket, 'key': key}
    return dml.Resource.from_dict(js)

@dataclass
class ShDagNS:
    dag: dml.Dag
    name = 'sh'

    def _resolve(self, obj: dml.Resource):
        return (CACHE_LOC/obj.info['id'])

    @staticmethod
    def _put_obj(src_path, file_id):
        fpath = CACHE_LOC/file_id
        if not (fpath).exists():
            os.rename(src_path, fpath)
            return True
        return False


    def tar_(self, src):
        tmpf = NamedTemporaryFile(prefix='dml-', delete=False)
        try:
            _hash = make_tarball(src, tmpf.name)
            file_id = f'{_hash}.tar.gz'
            if not self._put_obj(tmpf.name, file_id):
                os.unlink(tmpf.name)
        except Exception:
            os.unlink(tmpf.name)
            raise
        fndag = self.dag.start_fn(dml.Resource.from_dict({'exec': self.name}), 'local', file_id)
        if isinstance(fndag, dml.Node):
            logger.info('returning cached value')
            return fndag
        rsrc = dml.Resource.from_dict({'exec': self.name, 'id': file_id})
        logger.info('committing result')
        return fndag.commit(rsrc)

    def untar(self, rsrc: dml.Node, dest: Optional[str] = None):
        rsrc = self.dag.get_value(rsrc)
        tarball = self._resolve(rsrc)
        if dest is not None:
            return extract_tarball(tarball, dest)
        return tmp_untar(tarball)

    # def run(self):
    #     script, *args = self.dag.expr
    #     fndag = self.dag.start_fn(dml.Resource(self.name), tarball, dkr_path, cache=cache, retry=retry)
    #     if isinstance(fndag, dml.Node):
    #         return fndag
    #     tb_path = self.dag.get_value(tarball).info['path']
    #     ar_path = self.dag.get_value(dkr_path)
    #     resp = docker_build(tb_path, dkr_path=ar_path)
    #     rsrc = dml.Resource.from_dict({'id': ':'.join([resp['resource'], resp['tag']]), **resp})
    #     return fndag.commit(rsrc)

    def exists(self, obj: dml.Node):
        obj = self.dag.get_value(obj)
        return self._resolve(obj).is_file()


dml.Dag.register_ns(ShDagNS)


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

def docker_run(dkr: dict, script: str):
    with NamedTemporaryFile() as tmpf:
        cmd = [
            'docker', 'run',
            f'--mount=type=bind,source={script},target={script}',
            f'--mount=type=bind,source={tmpf.name},target={tmpf.name}',
        ]
        proc = subprocess.run([*cmd, dkr['id'], script, tmpf.name], capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f'{proc.returncode}:\n{proc.stderr}')
        with open(tmpf.name, 'r') as f:
            return dml.from_json(f.read())

@dataclass
class DkrDagNS:
    dag: dml.Dag
    name = 'dkr'

    def build(self, tarball: dml.Node, dkr_path: Optional[str|dml.Node] = None, *, cache: bool = True, retry: bool = False):
        """
        build a docker image

        Notes
        -----
        * This does not check to ensure the docker image exists locally.
          If you're working locally, you'll have to check it yourself and retry if not
        """
        if isinstance(dkr_path, dml.Node):
            ar_path = self.dag.get_value(dkr_path)
        else:
            ar_path = dkr_path
            dkr_path = self.dag.put(ar_path)
        fn_rsrc = dml.Resource.from_dict({'exec': self.name})
        fndag = self.dag.start_fn(fn_rsrc, tarball, dkr_path, cache=cache, retry=retry)
        if isinstance(fndag, dml.Node):
            return fndag
        with self.dag.sh.untar(tarball) as tb_path:
            resp = docker_build(tb_path, dkr_path=ar_path)
        rsrc = dml.Resource.from_dict({'id': f"{resp['repository']}@{resp['hash']}", **resp})
        return fndag.commit(rsrc)

    def exists(self, img: dml.Node):
        cmd = ['docker', 'image', 'inspect', self.dag.get_value(img).info['id']]
        print('command:', ' '.join(cmd))
        resp = subprocess.run(cmd, check=False)
        return resp.returncode == 0

    def run(self, img: dml.Node, *args: dml.Node):
        pass


dml.Dag.register_ns(DkrDagNS)
