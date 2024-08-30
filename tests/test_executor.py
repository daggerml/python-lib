from itertools import product
from time import sleep

import boto3
import pytest

import daggerml as dml
import daggerml.executor as dx
import tests.batch_executor as tba
from tests.util import Api, DmlTestBase

TEST_BUCKET = 'dml-test-misc2'
try:
    boto3.client('s3').list_objects_v2(Bucket=TEST_BUCKET)
    S3_ACCESS = True
except KeyboardInterrupt:
    raise
except Exception:
    S3_ACCESS = False


class TestLambda(DmlTestBase):

    def setUp(self):
        super().setUp()
        self._stack_name, lambda_arn = tba.up(self.id().replace('.', '-').replace('_', '-'))
        self._rsrc = dml.Resource(lambda_arn)

    def tearDown(self):
        tba.down(self._stack_name)
        super().tearDown()

    @pytest.mark.slow
    def test_invoke(self):
        nums = [2, 3, 5]
        with Api(initialize=True) as api:
            api.new_dag('lambda', 'creating lambda function').commit(self._rsrc)
            dag = api.new_dag('test-dag0', 'this is a test')
            rsrc_node = dag.load('lambda')
            waiter = dx.Lambda.run(dag, [rsrc_node, *nums])
            assert isinstance(waiter, dml.FnUpdater)
            result = None
            while result is None:
                result = waiter.update()
                sleep(10)
            assert isinstance(result, dml.Node)
            assert result.value() == [x + 1 for x in nums]
        with Api(initialize=True) as api:
            api.new_dag('lambda', 'creating lambda function').commit(self._rsrc)
            dag = api.new_dag('test-dag1', 'this is a test')
            rsrc_node = dag.load('lambda')
            # run again
            waiter = dx.Lambda.run(dag, [rsrc_node, *nums])
            assert isinstance(waiter, dml.FnUpdater)
            # waiter.update()
            result = waiter.get_result()
            assert isinstance(result, dml.Node)
            assert result.value() == [x + 1 for x in nums]


@pytest.mark.skipif(not S3_ACCESS, reason='No s3 access')
class TestS3(DmlTestBase):

    def setUp(self):
        super().setUp()
        self.api('branch', 'create', 'foo')
        self.api('branch', 'use', 'foo')
        self.s3 = dx.S3(TEST_BUCKET, 'testico')

    def tearDown(self):
        for js in self.api.jscall('index', 'list'):
            print(f'{js = }')
            self.api('index', 'delete', js['id'])
        self.api('branch', 'use', 'main')
        self.api('branch', 'delete', 'foo')
        uris = self.api('repo', 'gc').strip().split('\n')
        self.s3.delete(*uris)
        super().tearDown()

    def test_bytes(self):
        dag = self.new('dag0', 'message')
        rsrc = self.s3.put_bytes(dag, b'qwer')
        assert self.s3.get_bytes(rsrc) == b'qwer'
        self.assertCountEqual(self.s3.list(), [rsrc.value().uri])

    def test_local_remote_file(self):
        dag = self.new('dag0', 'message')
        content = 'asdf'
        with self.s3.tmp_remote(dag) as tmpf:
            with open(tmpf.name, mode='w') as f:
                f.write(content)
        assert isinstance(tmpf.result, dml.Node)
        assert self.s3.get_bytes(tmpf.result) == content.encode()
        node = self.s3.put_bytes(dag, content.encode())
        assert node.value().uri == tmpf.result.value().uri
        with self.s3.tmp_local(node) as tmpf:
            with open(tmpf, 'r') as f:
                assert f.read().strip() == content

    def test_polars_df(self):
        dag = self.new('dag0', 'msesages')
        import polars as pl
        df = pl.from_dicts([{'x': i, 'y': j} for i, j in product(range(5), repeat=2)])
        resp = self.s3.write_parquet(dag, df)
        uri = resp.value().uri
        assert uri.startswith('s3://')
        assert uri.endswith('.parquet')
        df = pl.from_dicts([{'x': i, 'y': j} for i, j in product(range(5), repeat=2)])
        resp = self.s3.write_parquet(dag, df)
        uri2 = resp.value().uri
        assert uri == uri2

    def test_pandas_df(self):
        dag = self.new('dag0', 'msesages')
        import pandas as pd
        df = pd.DataFrame([{'x': i, 'y': j} for i, j in product(range(5), repeat=2)])
        resp = self.s3.write_parquet(dag, df)
        uri = resp.value().uri
        assert uri.startswith('s3://')
        assert uri.endswith('.parquet')
        df = pd.DataFrame([{'x': i, 'y': j} for i, j in product(range(5), repeat=2)])
        resp = self.s3.write_parquet(dag, df, index=False)
        uri2 = resp.value().uri
        assert uri == uri2
        with self.s3.tmp_local(resp) as f:
            df2 = pd.read_parquet(f)
        assert df2.equals(df)

    def test_pandas_df_index(self):
        dag = self.new('dag0', 'msesages')
        import pandas as pd
        df = pd.DataFrame([{'x': i, 'y': j} for i, j in product(range(5), repeat=2)])
        df.index = pd.Index([f'x{i}' for i in df.index], name='IDX')
        resp = self.s3.write_parquet(dag, df)
        with self.s3.tmp_local(resp) as f:
            df2 = pd.read_parquet(f)
        assert df2.equals(df)
        resp = self.s3.write_parquet(dag, df, index=False)
        with self.s3.tmp_local(resp) as f:
            df2 = pd.read_parquet(f)
        assert not df2.equals(df)
