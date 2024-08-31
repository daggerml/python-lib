import json
import logging
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from hashlib import sha256
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

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

        def lambda_update_fn(cache_key, dump):
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
    def tmp_remote(self, dag, id=None):
        with TemporaryDirectory(prefix='dml-s3-') as tmpd:
            tmpf = f'{tmpd}/obj'
            obj = TmpRemote(tmpf)
            yield obj
            with open(tmpf, mode='rb') as f:
                id = id or f'{sha256(f.read()).hexdigest()}.bytes'
            to = f'{self.prefix}/{id}'
            self.client.upload_file(tmpf, self.bucket, to)
        obj.result = dag.put(dml.Resource(f's3://{self.bucket}/{to}'))

    def write_parquet(self, dag, df, **kw) -> dml.Node:
        hsh = sha256()
        if pl is not None and isinstance(df, pl.DataFrame):
            for x in df.hash_rows().sort():
                hsh.update(x.to_bytes(64, "little"))
            with self.tmp_remote(dag, id=f'{hsh.hexdigest()}.pl.parquet') as tmp:
                df.write_parquet(tmp.name, **kw)
            return tmp.result
        elif pd is not None and isinstance(df, pd.DataFrame):
            for x in pd.util.hash_pandas_object(df):
                hsh.update(x.to_bytes(64, 'little'))
            with self.tmp_remote(dag, id=f'{hsh.hexdigest()}.pd.parquet') as tmp:
                df.to_parquet(tmp.name, **kw)
            return tmp.result
        msg = f'Unrecognized type for write_parquet: {type(df) = }'
        raise ValueError(msg)
