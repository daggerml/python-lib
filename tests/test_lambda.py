from pathlib import Path

import boto3

import daggerml as dml
import daggerml.executor.lambda_ as lam
from tests.util import DmlTestBase

_here_ = Path(__file__).parent
TEST_BUCKET = 'amn-dgr'
try:
    boto3.client('s3').list_objects_v2(Bucket=TEST_BUCKET)
    S3_ACCESS = True
except KeyboardInterrupt:
    raise
except Exception:
    S3_ACCESS = False


class TestCore(DmlTestBase):

    def test_invoke(self):
        dag = dml.new('test-dag0', 'this is a test')
        # _id = f'dml-{uuid4().hex[:8]}'
        # zip_file = f'lambda/{_id}.zip'
        # subprocess.run(['zip', '-r', zip_file, '-x', '.git/*', '.dml/*', '__pycache__/*', 'tests/*', 'Makefile', 'lambda/*', '@', '.'])
        # boto3.client('s3').upload_file(zip_file, 'dml-test-misc2', f'test/{_id}.zip')
        # os.remove(zip_file)
        rsrc_node = dag.load('test-lambda')
        resp = lam.run(dag, [rsrc_node, 2], boto_session=boto3.Session(profile_name='misc2', region_name='us-west-2'))
        assert isinstance(resp, dml.Node)
        assert resp.value() == [rsrc_node.value(), 2]
