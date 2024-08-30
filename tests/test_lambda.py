from time import sleep

import boto3
import pytest

import daggerml as dml
import daggerml.executor.lambda_ as lam
import tests.batch_executor as tba
from tests.util import Api, DmlTestBase

TEST_BUCKET = 'amn-dgr'
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
            waiter = lam.run(dag, [rsrc_node, *nums])
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
            waiter = lam.run(dag, [rsrc_node, *nums])
            assert isinstance(waiter, dml.FnUpdater)
            # waiter.update()
            result = waiter.get_result()
            assert isinstance(result, dml.Node)
            assert result.value() == [x + 1 for x in nums]
