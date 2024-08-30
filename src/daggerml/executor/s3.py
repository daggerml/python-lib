"""
1. given an s3 object return a resource (no create)
2. 

"""
import logging
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from hashlib import sha256
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

import boto3

import daggerml as dml

logger = logging.getLogger(__name__)


@dataclass
class TmpRemote:
    name: str
    result: dml.Node|None = None


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

    def put_bytes(self, dag: dml.Dag, obj: bytes) -> dml.Node:
        _id = sha256(obj).hexdigest()
        key = f'{self.prefix}/{_id}.bytes'
        self.client.put_object(Body=obj, Bucket=self.bucket, Key=key)
        return dag.put(dml.Resource(f's3://{self.bucket}/{key}'))

    def get_bytes(self, resource: dml.Resource|dml.Node) -> bytes:
        if isinstance(resource, dml.Node):
            resource = resource.value()
            assert isinstance(resource, dml.Resource)
        parsed = urlparse(resource.uri)
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
    def tmp_local(self, resource: dml.Resource|dml.Node):
        if isinstance(resource, dml.Node):
            resource = resource.value()
            assert isinstance(resource, dml.Resource)
        parsed = urlparse(resource.uri)
        with TemporaryDirectory(prefix='dml-s3-') as tmpd:
            tmpf = f'{tmpd}/obj'
            self.client.download_file(parsed.netloc, parsed.path[1:], tmpf)
            yield tmpf

    @contextmanager
    def tmp_remote(self, dag):
        with TemporaryDirectory(prefix='dml-s3-') as tmpd:
            tmpf = f'{tmpd}/obj'
            obj = TmpRemote(tmpf)
            yield obj
            with open(tmpf, mode='rb') as f:
                id = f'{self.prefix}/{sha256(f.read()).hexdigest()}.bytes'
            self.client.upload_file(tmpf, self.bucket, id)
        obj.result = dag.put(dml.Resource(f's3://{self.bucket}/{id}'))
