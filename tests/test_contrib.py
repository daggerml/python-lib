#!/usr/bin/env python3
# import os
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from time import time

import boto3
import pytest

import daggerml as dml
import daggerml.contrib.s3 as s3
from daggerml import cached_executor
from daggerml._config import DML_S3_ENDPOINT, DML_TEST_LOCAL
from daggerml.contrib.local import hatch, local_fn
from tests.util import DmlTestBase


@hatch(env='test-env')
def hatch_check(dag):
    "hatch functions need to be globally accessible for now"
    import pandas as pd
    return pd.__version__


class TestLocalExecutor(DmlTestBase):

    def test_local_func_basic(self):
        @local_fn
        def add(dag, *args):
            return sum([x.to_py() for x in args])
        dag = dml.Dag.new(self.id())
        args = [1, 2, 3, 4, 5]
        resp = add(dag, *args, name='foo')
        assert resp.to_py() == sum(args)
        assert resp.meta['name'] == 'foo'

    def test_not_squashing(self):
        "make sure different functions aren't overwriting each others caches"
        @local_fn
        def sub(dag, c, d):
            return c.to_py() - d.to_py()
        @local_fn
        def add(dag, *args):
            return sum([x.to_py() for x in args])
        dag = dml.Dag.new(self.id())
        assert add(dag, 3, 1).to_py() == 3 + 1
        assert sub(dag, 3, 1).to_py() == 3 - 1

    def test_local_func_localex(self):
        le = cached_executor('foobar', 0)

        mutable = []
        @local_fn(executor_name=le.name, executor_version=le.version)
        def fn(dag, *args):  # noqa: B006
            mutable.append(None)
            return args  # len(mutable) - 1

        dag0 = dml.Dag.new(self.id())
        fn(dag0, 0).to_py()
        assert len(mutable) == 1
        fn(dag0, 0).to_py()
        assert len(mutable) == 1
        for i in range(5):
            fn(dag0, time()).to_py()
            assert len(mutable) == i + 2
        fn(dag0, 0).to_py()
        assert len(mutable) == 6
        fn(dag0, 0).to_py()
        assert len(mutable) == 6

        dag1 = dml.Dag.new(self.id())  # not the same executor
        fn(dag1, 0).to_py()
        assert len(mutable) == 6

        # we have to delete these dags so that teardown will work properly
        dag0.delete()
        dag1.delete()
        le.delete()

    def test_hatch(self):
        dag = dml.Dag.new(self.id())
        res = hatch_check(dag)
        assert res.to_py() == '2.0.1'


class TestS3Resource(DmlTestBase):

    def setUp(self):
        self.client = boto3.client('s3', endpoint_url=DML_S3_ENDPOINT)
        self.bucket = 'daggerml-base'
        self.prefix = 'test'
        if DML_TEST_LOCAL:
            self.client.create_bucket(Bucket=self.bucket)

    def test_no_bucket(self):
        dag = dml.Dag.new(self.id())
        with pytest.raises(ValueError, match='upload requires'):
            s3.upload(dag, b'this is a test', bucket=None, prefix=self.prefix, client=self.client)

    def test_upload_basic(self):
        dag = dml.Dag.new(self.id())
        obj = b'this is a test'
        resource_node = s3.upload(dag, obj, bucket=self.bucket, prefix=self.prefix, client=self.client)
        try:
            assert isinstance(resource_node, dml.Node)
            resource = resource_node.to_py()
            assert isinstance(resource, s3.S3Resource)
            assert resource.tag == 'com.daggerml.executor.s3'
            assert resource.uri.startswith('s3://daggerml-base/test/')
            assert resource.bucket == self.bucket
            assert resource.key.startswith('test/')
            resp = self.client.get_object(
                Bucket=resource.bucket,
                Key=resource.key
            )
            assert resp['Body'].read() == obj
        finally:
            resp = self.client.delete_object(
                Bucket=resource.bucket,
                Key=resource.key
            )

    def test_s3_etag_small_file(self):
        dag = dml.Dag.new(self.id())
        txt = b'this is a test'
        resource_node = s3.upload(dag, txt, bucket=self.bucket, prefix=self.prefix, client=self.client)
        rsrc = resource_node.to_py()
        etag = rsrc.key.split('/')[-1]
        assert self.client.head_object(Bucket=rsrc.bucket, Key=rsrc.key)['ETag'] == f'"{etag}"'
        self.client.delete_object(
            Bucket=rsrc.bucket,
            Key=rsrc.key
        )

    def test_upload_directory(self):
        dag = dml.Dag.new(self.id())
        # upload the test directory
        rsrc = s3.tar(dag, '.', self.bucket, self.prefix, self.client).to_py()
        with TemporaryDirectory(prefix='dml-') as tmpd:
            tmpd = Path(tmpd)
            data_dir = tmpd / 'data'
            data_dir.mkdir()
            # download tarbell
            self.client.download_file(rsrc.bucket, rsrc.key, tmpd / 'data.tar.gz')
            # extract a copy of this directory, and the hash (uri) should be the same
            subprocess.run(f'tar -xzf {tmpd}/data.tar.gz -C {data_dir}/', shell=True)
            rsrc2 = s3.tar(dag, data_dir, self.bucket, self.prefix, self.client).to_py()
            assert rsrc.uri == rsrc2.uri
            # add a file and the hash should change
            with open(data_dir / 'foo.txt', 'w') as f:
                f.write('testico')
            rsrc4 = s3.tar(dag, data_dir, self.bucket, self.prefix, self.client)
            assert rsrc.uri != rsrc4.to_py().uri
