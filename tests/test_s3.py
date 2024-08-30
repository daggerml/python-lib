
import boto3

import daggerml as dml
from daggerml.executor.s3 import S3
from tests.util import DmlTestBase

TEST_BUCKET = 'dml-test-misc2'
try:
    boto3.client('s3').list_objects_v2(Bucket=TEST_BUCKET)
    S3_ACCESS = True
except KeyboardInterrupt:
    raise
except Exception:
    S3_ACCESS = False


class TestCore(DmlTestBase):

    def setUp(self):
        super().setUp()
        self.api('branch', 'create', 'foo')
        self.api('branch', 'use', 'foo')
        self.s3 = S3(TEST_BUCKET, 'testico')

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
