#!/usr/bin/env python
import hashlib
import json
import os
from io import BytesIO

from daggerml import Dag, delete_dag


class local_executor:
    def __init__(self, name, version):
        self.name = name
        self.version = version

    @property
    def file_path(self):
        return f'.dml/{self.name}_{self.version}.json'

    def get(self, dag):
        exec_dag = Dag.new(self.name, self.version)
        if exec_dag is not None:
            exec_dag.commit(exec_dag.executor)
            with open(self.file_path, 'w') as fo:
                json.dump(vars(exec_dag), fo)
            secret = exec_dag.secret
        else:
            with open(self.file_path, 'r') as fo:
                secret = json.load(fo)['secret']
        exec_node = dag.load(self.name, version=self.version)
        return exec_node.to_py(), secret

    def delete(self):
        with open(self.file_path, 'r') as fo:
            js = json.load(fo)
        delete_dag(js['id'])
        os.remove(self.file_path)
        return


def fullname(obj):
    return '.'.join([obj.__module__, obj.__qualname__])


def compute_hash(file_obj, chunk_size=8*1024*1024):
    """compute hash consistent with aws s3 etag of file"""
    # unconfirmed from https://stackoverflow.com/a/43819225
    if isinstance(file_obj, bytes):
        return compute_hash(BytesIO(file_obj), chunk_size)
    elif isinstance(file_obj, (str, os.PathLike)):
        with open(file_obj, 'rb') as fo:
            return compute_hash(fo, chunk_size)
    md5s = []
    while data := file_obj.read(chunk_size):
        if data is None:
            break
        md5s.append(hashlib.md5(data))
    if len(md5s) < 1:
        return hashlib.md5().hexdigest()
    if len(md5s) == 1:
        return md5s[0].hexdigest()
    digests = b''.join(m.digest() for m in md5s)
    digests_md5 = hashlib.md5(digests)
    return '{}-{}'.format(digests_md5.hexdigest(), len(md5s))  # wrap this in double quotes for s3 etag
