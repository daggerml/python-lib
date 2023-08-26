#!/usr/bin/env python
import hashlib
import logging
import os
import subprocess
import sys
import tarfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

import boto3

from daggerml._config import DML_S3_BUCKET, DML_S3_PREFIX
from daggerml._dag import Resource, register_tag

logger = logging.getLogger(__name__)


@register_tag('com.daggerml.executor.s3')
@dataclass(frozen=True)
class S3Resource(Resource):
    """data on s3"""
    @property
    def uri(self):
        return self.id

    @property
    def bucket(self):
        return urlparse(self.uri).netloc

    @property
    def key(self):
        return urlparse(self.uri).path[1:]

    @classmethod
    def from_uri(cls, parent, uri):
        return cls(id=uri, parent=parent, tag='com.daggerml.executor.s3')


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


def upload(dag, bytes_object, bucket=DML_S3_BUCKET, prefix=DML_S3_PREFIX, client=None):
    """upload data to s3"""
    if bucket is None:
        raise ValueError('`s3.upload requires `DML_S3_{BUCKET,PREFIX}` to be set')
    prefix = prefix.rstrip('/')
    if client is None:
        client = boto3.client('s3')
    key = f'{prefix}/bytes/{compute_hash(bytes_object)}'
    resource = S3Resource.from_uri(dag.executor, f's3://{bucket}/{key}')
    client.put_object(Body=bytes_object, Bucket=bucket, Key=key)
    return dag.from_py(resource)


def tar(dag, path, bucket, prefix, client=None):
    if client is None:
        client = boto3.client('s3')
    path = Path(path).expanduser()
    if not path.is_absolute():
        # if not abspath, then it's relative to the calling function
        calling_file = sys._getframe(1).f_globals['__file__']
        path = Path(calling_file).parent / path
        path = path.absolute()
        logger.info('set path relative to %r -- %r', calling_file, path)
    if not path.is_dir():
        raise ValueError('path %s is not a valid directory' % path)
    hash_script = Path(__file__).parent / 'extras/local-dir-hash.sh'
    dir_hash = subprocess.run([str(hash_script), str(path)], capture_output=True, shell=True)\
        .stdout.decode().strip()
    key = f'{prefix}/s3-upload/{dir_hash}.tar.gz'
    uri = f's3://{bucket}/{key}'
    logger.info('compressing and uploading %r to %r', path, uri)
    with NamedTemporaryFile(dir='/tmp/', suffix='.tar.gz', prefix='dml-s3-upload') as f:
        with tarfile.open(f.name, 'w:gz') as tar:
            tar.add(path, arcname=os.path.sep)
        client.put_object(
            Body=f.read(), Bucket=bucket, Key=key
        )
    return dag.from_py(S3Resource.from_uri(dag.executor, uri))
