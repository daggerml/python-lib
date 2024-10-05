import os
import unittest
from glob import glob
from itertools import product
from pathlib import Path
from shutil import which
from tempfile import TemporaryDirectory
from unittest.mock import patch

import boto3

import daggerml as dml
import daggerml.executor as dx
import tests.batch_executor as bx
from tests.util import SYSTEM, DmlTestBase

TEST_BUCKET = "dml-s3-test-doesnotexist"
TEST_PREFIX = "testico"

_root_ = Path(__file__).parent.parent


def rel_to(x, rel):
    return str(Path(x).relative_to(rel))


def ls_r(path):
    return [rel_to(x, path) for x in glob(f"{path}/**", recursive=True)]


class MotoTestBase(DmlTestBase):

    def setUp(self):
        # clear out env variables for safety
        for k in sorted(os.environ.keys()):
            if k.startswith("AWS_"):
                del os.environ[k]
        os.environ["AWS_ACCESS_KEY_ID"] = "foobar"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foobar"
        os.environ["AWS_REGION"] = os.environ["AWS_DEFAULT_REGION"] = "us-west-2"
        # this loads env vars, so import after clearing
        from moto.server import ThreadedMotoServer
        super().setUp()
        self.server = ThreadedMotoServer(port=0)
        self.server.start()
        self.moto_host, self.moto_port = self.server._server.server_address
        self.endpoint = f"http://{self.moto_host}:{self.moto_port}"
        os.environ["AWS_ENDPOINT_URL"] = self.endpoint
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=TEST_BUCKET)

    def tearDown(self):
        self.server.stop()
        super().tearDown()


class TestS3(MotoTestBase):

    def test_bytes(self):
        s3 = dx.S3(TEST_BUCKET, TEST_PREFIX)
        dag = self.new("dag0", "message")
        rsrc = s3.put_bytes(dag, b"qwer")
        assert s3.get_bytes(rsrc) == b"qwer"
        self.assertCountEqual(s3.list(), [rsrc.value().uri])

    def test_local_remote_file(self):
        s3 = dx.S3(TEST_BUCKET, TEST_PREFIX)
        dag = self.new("dag0", "message")
        content = "asdf"
        with s3.tmp_remote(dag) as tmpf:
            with open(tmpf.name, mode="w") as f:
                f.write(content)
        assert isinstance(tmpf.result, dml.Node)
        assert s3.get_bytes(tmpf.result) == content.encode()
        node = s3.put_bytes(dag, content.encode())
        assert node.value().uri == tmpf.result.value().uri
        with s3.tmp_local(node) as tmpf:
            with open(tmpf, "r") as f:
                assert f.read().strip() == content

    def test_polars_df(self):
        s3 = dx.S3(TEST_BUCKET, TEST_PREFIX)
        dag = self.new("dag0", "msesages")
        import polars as pl
        df = pl.from_dicts([{"x": i, "y": j} for i, j in product(range(5), repeat=2)])
        resp = s3.write_parquet(dag, df)
        uri = resp.value().uri
        assert uri.startswith('s3://')
        assert uri.endswith('.parquet')
        df = pl.from_dicts([{'x': i, 'y': j} for i, j in product(range(5), repeat=2)])
        resp = s3.write_parquet(dag, df)
        uri2 = resp.value().uri
        assert uri == uri2

    def test_pandas_df(self):
        s3 = dx.S3(TEST_BUCKET, TEST_PREFIX)
        dag = self.new('dag0', 'msesages')
        import pandas as pd
        df = pd.DataFrame([{'x': i, 'y': j} for i, j in product(range(5), repeat=2)])
        resp = s3.write_parquet(dag, df)
        uri = resp.value().uri
        assert uri.startswith('s3://')
        assert uri.endswith('.parquet')
        df = pd.DataFrame([{'x': i, 'y': j} for i, j in product(range(5), repeat=2)])
        resp = s3.write_parquet(dag, df, index=False)
        uri2 = resp.value().uri
        assert uri == uri2
        with s3.tmp_local(resp) as f:
            df2 = pd.read_parquet(f)
        assert df2.equals(df)

    def test_pandas_df_index(self):
        s3 = dx.S3(TEST_BUCKET, TEST_PREFIX)
        dag = self.new('dag0', 'msesages')
        import pandas as pd
        df = pd.DataFrame([{'x': i, 'y': j} for i, j in product(range(5), repeat=2)])
        df.index = pd.Index([f'x{i}' for i in df.index], name='IDX')
        resp = s3.write_parquet(dag, df)
        with s3.tmp_local(resp) as f:
            df2 = pd.read_parquet(f)
        assert df2.equals(df)
        resp = s3.write_parquet(dag, df, index=False)
        with s3.tmp_local(resp) as f:
            df2 = pd.read_parquet(f)
        assert not df2.equals(df)

    def test_tarball(self):
        s3 = dx.S3(TEST_BUCKET, TEST_PREFIX)
        def exclude_tests(x):
            if x.name.startswith('.'):  # no hidden files should be necessary for this
                return None
            if '__pycache__' in x.name:
                return None
            if x.name.startswith('submodule'):
                return None
            if x.name.startswith('tests/'):
                return None
            return x
        dag = self.new('dag0', 'foopy')
        node = s3.tar(dag, _root_, filter_fn=exclude_tests)
        with TemporaryDirectory(prefix='dml-tests-') as tmpd:
            s3.untar(node, tmpd)
            contents = set(ls_r(tmpd))
            # not empty
            assert 'src/daggerml/core.py' in contents
            # no extra assets
            assert contents < set(ls_r(_root_))
            # filter is working
            assert all(not y.startswith('tests/') for y in contents)


@unittest.skipIf(which("docker") is None, "docker is not available.")
class TestDocker(MotoTestBase):

    @classmethod
    def setUpClass(cls):
        cls.dkr_name = 'dml_test'
        flags = [
            '-f', 'tests/assets/Dockerfile',
            '-t', cls.dkr_name,
        ]
        if SYSTEM == 'darwin':
            flags.extend(['--platform=linux/amd64', '--load'])
        cls.dkr_img_id = dx._dkr_build(_root_, flags)

    def test_local(self):
        dkr = dx.Dkr()
        def exclude_tests(x):
            if any(y.startswith('.') for y in x.name.split('/')):
                return None
            if '__pycache__' in x.name:
                return None
            if 'tests/' in x.name and 'assets' not in x.name:
                return None
            return x
        dag = self.new('dag0', 'foopy')
        s3 = dx.S3(TEST_BUCKET, TEST_PREFIX)
        node = s3.tar(dag, _root_, filter_fn=exclude_tests)
        with patch('daggerml.executor._dkr_build', return_value=self.dkr_img_id):
            img = dkr.build(dag, node, ['-f', 'tests/assets/Dockerfile'], s3).get_result()
        assert isinstance(img, dml.Node)

        def add_one(fndag):
            _, *nums = fndag.expr.value()
            return dag.commit([x + 1 for x in nums])
        script = s3.scriptify(dag, add_one)
        flags = [
            '--add-host=host.docker.internal:host-gateway',
            '-e', f'AWS_ENDPOINT_URL=http://host.docker.internal:{self.moto_port}',
            '--platform=linux/amd64',
        ]
        fn = (
            dkr
            .make_fn(dag, img, script, flags)
            .get_result()
        )
        assert isinstance(fn, dml.Node)
        nums = [1, 2, 1, 5]
        result = dkr.run(dag, fn, *nums, s3=s3).get_result()
        assert isinstance(result, dml.Node)
        assert result.value() == [x + 1 for x in nums]

    # @unittest.skipIf(SYSTEM == "darwin", "moto impl of lambda doesn't work on mac (docker stuff)")
    @unittest.skip("I haven't figured out how to get moto's lambda to communicate with moto server (to submit jobs)")
    def test_remote(self):
        """There are two issues here (both above)"""
        dkr = dx.Dkr()
        s3 = dx.S3(TEST_BUCKET, TEST_PREFIX)
        lam = dx.Lambda()
        resp = bx.up_cluster(TEST_BUCKET)
        r2 = bx.up_jobdef(f'{self.dkr_name}:latest')
        def exclude_tests(x):
            return None
        dag = self.new('dag0', 'foopy')
        node = s3.tar(dag, _root_, filter_fn=exclude_tests)
        with patch('daggerml.executor._dkr_build', return_value=self.dkr_img_id):
            img = dkr.build(dag, node, ['-f', 'tests/assets/Dockerfile'], s3).get_result()
        with patch('daggerml.executor.Ecr._push', return_value=f'{self.dkr_name}:latest'):
            img = dx.Ecr('').push(dag, img)
        tmp = dict(**resp, **r2)
        _lam = dml.Resource(tmp['LambdaArn'])
        _jd = dml.Resource(tmp['JobDef'])
        _jq = dml.Resource(tmp['JobQueue'])

        def add_one(fndag):
            _, *nums = fndag.expr.value()
            return dag.commit([x + 1 for x in nums])
        script = s3.scriptify(dag, add_one)
        fn = (
            lam
            .make_fn(dag, _lam, _jq, _jd, script)
            .get_result()
        )
        nums = [1, 2, 1, 5]
        result = lam.run(dag, fn, *nums).get_result()
        assert isinstance(result, dml.Node)
        assert result.value() == [x + 1 for x in nums]
