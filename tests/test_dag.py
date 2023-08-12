from dataclasses import replace
from datetime import datetime
from time import sleep
from uuid import uuid4

import boto3
import pytest
from util import DmlTestBase

import daggerml as dml
from daggerml import (
    ApiError,
    Dag,
    DagError,
    Node,
    NodeError,
    Resource,
    S3Resource,
    dag_fn,
    describe_dag,
    list_dags,
    s3_upload,
)
from daggerml._dag import _api


def get_dag(dag):
    for d in dml.list_dags(name=dag.name):
        if d['version'] == dag.version:
            return d
    raise Exception(f'dag not found: {dag}')


def dag_success(dag):
    d = get_dag(dag)
    return d['complete'] and not d['failed']


def dag_failed(dag):
    d = get_dag(dag)
    return d['complete'] and d['failed']


def dag_incomplete(dag):
    d = get_dag(dag)
    return not d['complete']


def dag_failure_info(dag):
    d = get_dag(dag)
    return d['failure_info']


# NOTE: The DmlTestBase class has a tearDown() method which deletes dags with
# names starting with self.id().

class TestDagBasic(DmlTestBase):

    def test_dag_new(self):
        d0 = Dag.new(self.id())

        # Dag.new() returns an instance of Dag:
        assert isinstance(d0, Dag)
        # The dag is incompllete because we have not committed a value:
        assert dag_incomplete(d0)

    def test_dag_commit(self):
        d0 = Dag.new(self.id())
        d0.commit(42)

        # The dag is complete and has a result because we committed a value:
        assert dag_success(d0)

        # Committing a dag which was already committed should raise:
        with pytest.raises(ApiError, match='dag already succeeded'):
            d0.commit(42)

        # Failing a dag which was already committed should raise:
        with pytest.raises(ApiError, match='dag already succeeded'):
            d0.fail()

    def test_dag_fail_explicit(self):
        d0 = Dag.new(self.id())
        d0.fail()

        assert dag_failed(d0)
        assert dag_failure_info(d0) == {}

        # Failing a dag which already failed should raise:
        with pytest.raises(ApiError, match='dag already failed'):
            d0.fail()

        # Committing a dag which already failed should raise:
        with pytest.raises(ApiError, match='dag already failed'):
            d0.commit(42)

    def test_dag_fail_with_info(self):
        d0 = Dag.new(self.id())
        d0.fail({'error': 'you are a soup sandwich'})

        assert dag_failed(d0)
        assert dag_failure_info(d0) == {'error': 'you are a soup sandwich'}

    def test_dag_version_sequence(self):
        # We'll be starting multiple dags with the same name:
        name = self.id()

        # First dag with a given name and no version specified should get version 1:
        d0 = Dag.new(name)
        assert d0.version == 1

        # Next dag with the same name and no version specified should get version 1:
        d1 = Dag.new(name)
        assert d1.version == 2

        # Delete the dags:
        d0.delete()
        d1.delete()

        # Dag gets version 1 because we deleted the other dags with this name:
        assert Dag.new(name).version == 1

        # A version can be specified when starting a new dag:
        assert Dag.new(name, 77).version == 77

        # If the specified name and version already exists the result is None:
        assert Dag.new(name, 77) is None

        # Default version is one more than the highest version:
        assert Dag.new(name).version == 78

        # Dags don't need to be created in ascending version order:
        assert Dag.new(name, 15).version == 15  # FIXME: Is this desired behavior?

        # Default version is still one more than the highest version, even
        # when the most recently created dag didn't have the highest version:
        assert Dag.new(name).version == 79

    def test_dag_from_py_and_to_py(self):
        d0 = Dag.new(self.id())
        n0 = d0.from_py(42)

        # The dag from_py() method returns a Node instance:
        assert isinstance(n0, Node)

        # The to_py() method of Dag and Node both return the python value:
        assert n0.to_py() == d0.to_py(n0) == 42

    def test_dag_various_literal_node_values(self):
        d0 = Dag.new(self.id())

        # Various cases testing different types of literal values:
        test_cases = [
            None,
            1,
            1.1,
            True,
            False,
            'foop',
            (1, 'barp', ('bazp'), None),
            {'foo': 'bar', 'baz': (1, 2, {'baf': None})},
        ]

        for test_case in test_cases:
            n = d0.from_py(test_case)
            # N should be a literal node:
            assert isinstance(n, Node)
            # There are two ways to get the python value of a Node:
            assert d0.to_py(n) == n.to_py() == test_case

    def test_dag_literal_node_scalar_no_such_key(self):
        d0 = Dag.new(self.id())

        # Create a node in d0 with scalar value:
        n0 = d0.from_py(2)

        # Access the 0th item of a scalar valued node, which should raise:
        with pytest.raises(NodeError, match='no such key'):
            n0[0]

        # Access the 'asdf' key of a scalar valued node, which should raise:
        with pytest.raises(NodeError, match='no such key'):
            n0['asdf']

    def test_dag_literal_node_list(self):
        d0 = Dag.new(self.id())

        # Create a python list:
        c0 = [1, 2, 3]

        # Create a node in d0 with list value:
        n0 = d0.from_py(c0)

        # Should be able to get the length of the list without using to_py():
        assert len(n0) == len(c0)

        # Accessing an item in the list node returns a Node instance:
        assert isinstance(n0[0], Node)

        # The python value of the item node should be as expected:
        assert n0[0].to_py() == c0[0]

        # Access a string key of a list, should raise:
        with pytest.raises(NodeError, match='no such key'):
            n0['asdf']

        # Access index which is out of range, should raise:
        with pytest.raises(NodeError, match='no such key'):
            n0[len(n0)]

    def test_dag_literal_node_list_concatenate(self):
        d0 = Dag.new(self.id())

        # Create two python lists:
        l0 = [1, 'asdf']
        l1 = ['qwer', 23]

        # Create a new python tuple by concatenating l0 and l1:
        l2 = tuple(l0 + l1)

        # Create two list nodes in d0:
        n0 = d0.from_py(l0)
        n1 = d0.from_py(l1)
        # Create a new list node in d0 by concatenating n0 and n1
        n2 = n0 + n1

        # The python value of the concatenated nodes should be the same as the tuple.
        assert n2.to_py() == l2

    def test_dag_literal_node_map(self):
        # Similar to list literal node test above:
        d0 = Dag.new(self.id())
        m0 = {'a': 1, 'b': 2, 'c': 3}
        n0 = d0.from_py(m0)

        assert isinstance(n0, Node)
        assert len(n0) == len(m0)
        assert n0['a'].to_py() == m0['a']
        with pytest.raises(NodeError, match='no such key'):
            n0[1]
        with pytest.raises(NodeError, match='no such key'):
            n0['d']

    def test_dag_literal_node_resource(self):
        d0 = Dag.new(self.id())
        n0 = d0.from_py(Resource('asdf', d0.executor, tag='blah'))

        # Every dag has an executor whose tag is the dag's name:
        assert d0.executor.tag == self.id()
        assert isinstance(n0, Node)
        assert isinstance(n0.to_py(), Resource)
        assert n0.to_py().parent == d0.executor

        # A new literal resource must have a tag and can only be created
        # with the dag's executor as its parent. Any other parent (or no
        # parent) should raise:

        # Executor is good but new resource can't have a tag:
        with pytest.raises(ApiError, match='new resources must have a tag'):
            d0.from_py(Resource('qwer', d0.executor, None))

        # Trying to create a new executor resource should raise:
        with pytest.raises(ApiError, match='resource that was not already in the dag'):
            d0.from_py(Resource('zxcv', None, 'my.problematic.tag'))

        # Trying to smuggle a resource into a dag should raise:
        with pytest.raises(ApiError, match='resource that was not already in the dag'):
            d1 = Dag.new(self.id() + '.other')
            d0.from_py(Resource('poiu', d1.executor, tag='test'))

    def test_dag_load(self):
        d0 = Dag.new(self.id())
        d0.commit('hello world')

        d1 = Dag.new(self.id() + '.other')
        n1 = d1.load(d0.name)

        assert isinstance(n1, Node)
        assert n1.to_py() == 'hello world'

    def test_dag_load_nonexistent(self):
        d0 = Dag.new(self.id())

        with pytest.raises(DagError, match='No such dag/version'):
            d0.load(uuid4().hex)

    def test_dag_topy_external_node(self):
        d0 = Dag.new(self.id())
        d1 = Dag.new(self.id() + '.other')
        n1 = d1.from_py({'asdf': 2})

        with pytest.raises(ValueError, match='node does not belong to dag'):
            d0.to_py(n1)

    def test_dag_frompy_external_node(self):
        d0 = Dag.new(self.id())
        d1 = Dag.new(self.id() + '.other')
        n1 = d1.from_py(d1.executor)

        with pytest.raises(ApiError, match='resource that was not already in the dag'):
            d0.from_py(n1)

    def test_dag_commit_external_node(self):
        d0 = Dag.new(self.id())
        d1 = Dag.new(self.id() + '.other')
        n1 = d1.from_py(d1.executor)

        with pytest.raises(ApiError, match='cannot commit a node from another dag'):
            _api('dag', 'commit_dag', dag_id=d0.id, result=n1.id, secret=d0.secret)

    def test_dag_func_application_basic(self):
        # First dag creates a func. In the real world this will be done once
        # when the user installs the executor. For example a func which exe-
        # cutes a job in a spark cluster will need to have a dag which stands
        # up the spark cluster and commits the func that other dags will use
        # to kick off jobs in that cluster. That's what's happening here, but
        # the spark cluster is just a mock that's running in the same process.
        d0 = Dag.new(self.id())
        d0.commit([d0.executor])

        # Second dag loads the func and calls it asynchronously:
        d1 = Dag.new(self.id() + '.user')
        f1 = d1.load(d0.name)
        v1 = f1.call_async(1, 2, 3)

        # This is the func implementation, in the real world this will be a
        # persistent service which loops forever claiming func applications
        # and committing the result. For this test we claim it here in the
        # same process, but imagine this is running in the cloud somewhere.
        with Dag.from_claim(d0.executor, d0.secret, 1) as d:
            argv = d.expr.to_py()[1::]
            d.commit(sum(argv))

        # Back in the second dag, the func application has a result now:
        v1 = v1.wait()
        d1.commit(v1)

        assert v1.to_py() == 1 + 2 + 3

    def test_dag_func_application_wrong_executor_frompy(self):
        d0 = Dag.new(self.id())

        d1 = Dag.new(self.id() + '.user')
        d1 = replace(d1, executor_id=d0.executor_id, secret=d0.secret)

        with pytest.raises(ApiError, match='bad secret or invalid executor'):
            d1.from_py(2)

    def test_dag_func_application_wrong_executor_commit(self):
        d0 = Dag.new(self.id())

        d1 = Dag.new(self.id() + '.user')
        n1 = d1.from_py(1)
        d1 = replace(d1, executor_id=d0.executor_id, secret=d0.secret)

        with pytest.raises(ApiError, match='bad secret or invalid executor'):
            d1.commit(n1)

class TestFuncApplication(DmlTestBase):

    def setUp(self):
        super().setUp()
        d0 = Dag.new(self.id())
        d0.commit([d0.executor])
        self.dag = d0
        self.executor = d0.executor
        self.secret = d0.secret
        self.fnum = 0

    def f(self, *args, **meta):
        d1 = Dag.new(self.id() + '.user' + str(self.fnum))
        self.fnum = self.fnum + 1
        f = d1.load(self.id())
        return f.call_async(*args, **meta)

    def claim(self, ttl=1, node_id=None):
        return Dag.from_claim(self.executor, self.secret, ttl, node_id=node_id)

    def test_func_application_nothing_to_claim(self):
        assert self.claim() is None

    def test_func_application_basic(self):
        y = self.f(1, 2, 3)

        with self.claim() as d:
            args = d.expr.to_py()[1::]
            d.commit(sum(args))

        y = y.wait()

        assert isinstance(y, Node)
        assert y.to_py() == sum(args)

    def test_func_application_refresh_claim(self):
        self.f(1)

        with self.claim(ttl=1) as d:
            expires = []

            # Refresh the claim before it has expired, multiple times.
            for ttl in [4, 20, 1]:
                r = d.refresh(ttl=ttl)

                assert isinstance(r, dict)
                assert sorted(r.keys()) == ['expiration', 'refresh_token']

                expires.append(datetime.fromtimestamp(r['expiration']))

            assert expires[0] < expires[1]
            assert expires[1] == expires[2]

    def test_func_application_timeout_reclaim(self):
        y = self.f(1)

        d0 = self.claim(ttl=1)

        # Claim hasn't expired yet, so we shouldn't be able to claim it again.
        assert self.claim() is None

        # Save expr because this dag is going to be deleted when the node is
        # reclaimed after this claim expires and to_py() is an operation that
        # is recorded as a node in the dag. If the dag is deleted you can't do
        # to_py() on any of its nodes.
        expr0 = d0.expr.to_py()

        # wait for claim to expire...
        sleep(1)

        # Claim the fnapp dag:
        d1 = self.claim()

        # Claim should be successful:
        assert isinstance(d1, Dag)
        assert isinstance(d1.expr, Node)

        # And we should have claimed a new fnapp dag with a new dag executor:
        assert d1.id != d0.id
        assert d1.executor_id != d0.executor_id

        # But the expr and secret of the new and old fnapp dags should be the same:
        assert d1.expr.to_py() == expr0
        assert d1.secret == d0.secret

        # Refresh claim should succeed for the new fnapp dag:
        assert isinstance(d1.refresh(), dict)

        # Refreshing a lost claim should raise (fnapp dag should have been deleted):
        with pytest.raises(ApiError, match='bad secret or invalid executor'):
            d0.refresh()

        # Committing with a lost claim should raise (fnapp dag should have been deleted):
        with pytest.raises(ApiError, match='bad secret or invalid executor'):
            d0.commit(self.id())

        # Commit the fnapp dag:
        d1.commit(42)

        # The func application should now have have a result:
        y = y.wait()

        assert isinstance(y, Node)
        assert y.to_py() == 42

    def test_func_application_explicit_fail(self):
        y = self.f('foop', 2)

        with self.claim() as d:
            d.fail({'message': 'error in node: ' + d.expr.id})

        # A failed func application should raise when complete:
        with pytest.raises(NodeError, match='error in node: ' + d.expr.id):
            y.wait()

    def test_func_application_exception_fail(self):
        y = self.f(1, 0)

        with self.claim() as d:
            argv = d.expr.to_py()
            d.commit(argv[1] / argv[2])

        with pytest.raises(NodeError, match='division by zero') as err:
            y.wait()

        assert isinstance(err.value.msg['trace'], list)

    def test_func_application_caching(self):
        y = self.f(1, 2)

        with self.claim() as d:
            argv = d.expr.to_py()
            d.commit(argv[1] / argv[2])

        assert y.wait().to_py() == 0.5

        # Func application should be cached so returns immediately without an
        # fnapp dag or executor doing any work:
        y = self.f(1, 2)
        assert y.wait().to_py() == 0.5

    def test_func_application_with_metadata(self):
        # Func application kwargs are attached to the fnapp node as metadata.
        y1 = self.f(1, 2, asdf=42)

        with self.claim() as d:
            # Fnapp node metadata is accessible via the fnapp dag expr.
            assert d.expr.meta['asdf'] == 42
            argv = d.expr.to_py()
            d.commit(argv[1] / argv[2])

        # We should have a result for y1 now.
        y1 = y1.wait()

        # Func applications w/ metadata are cached but metadata does not con-
        # tribute to the cache key, so y2 gets the cached result immediately.
        # Note that y1 and y2 are distinct nodes in the dag because metadata
        # does contribute to the node ID.
        y2 = self.f(1, 2, asdf=[1, 2, 3]).wait()

        # Should get the same result for y1 and y2.
        assert y1.to_py() == y2.to_py() == 0.5

        # The y1 and y2 should be different nodes in the dag.
        assert y1.id != y2.id

        # Nodes should have the correct metadata attached.
        assert y1.meta['asdf'] == 42
        assert y2.meta['asdf'] == [1, 2, 3]

    def test_func_application_volatile(self):
        # Call f without args the first time, func commits 42.
        y1 = self.f()
        self.claim().commit(42)

        # Since f isn't cached, func is called again and commits 43 this time.
        y2 = self.f()
        self.claim().commit(43)

        # The y1 and y2 nodes have distinct values:
        assert y1.wait().to_py() == 42
        assert y2.wait().to_py() == 43

    def test_func_application_volatile_with_metadata(self):
        # Zero arity func applications are volatile even with the same metadata.
        y1 = self.f(magic=42)
        y2 = self.f(magic=42)
        y3 = self.f(magic=43)

        # There should be three nodes to claim because none are cached.
        for _ in range(3):
            with self.claim() as d:
                d.commit(f'{d.expr.meta["magic"]} is magic')

        assert y1.wait().to_py() == '42 is magic'
        assert y2.wait().to_py() == '42 is magic'
        assert y3.wait().to_py() == '43 is magic'

class TestQuery(DmlTestBase):

    def test_query_list_without_name(self):
        Dag.new(self.id()).commit(23)
        resp = list_dags()
        assert isinstance(resp, list)
        assert len(resp) > 1

    def test_query_list_with_name(self):
        Dag.new(self.id()).commit(23)
        Dag.new(self.id()).commit(45)
        resp = list_dags(self.id())
        assert isinstance(resp, list)
        assert len(resp) == 2

    def test_query_describe_dag(self):
        Dag.new(self.id()).commit(23)
        resp = list_dags(self.id())
        dag_id = resp[0]['id']
        resp = describe_dag(dag_id)
        assert isinstance(resp, dict)
        assert resp['name'] == self.id()

class TestLocalExecutor(DmlTestBase):

    def test_local_func_basic(self):
        @dag_fn
        def add(dag):
            _, _, *args = dag.expr
            return sum([x.to_py() for x in args])
        dag = Dag.new(self.id())
        args = [1, 2, 3, 4, 5]
        resp = add(dag, *args)
        assert resp.to_py() == sum(args)

    def test_not_squashing(self):
        "make sure different functions aren't overwriting each others caches"
        @dag_fn
        def sub(dag):
            _, _, c, d = dag.expr
            return c.to_py() - d.to_py()

        @dag_fn
        def add(dag):
            _, _, *args = dag.expr
            return sum([x.to_py() for x in args])
        dag = Dag.new(self.id())
        add_resp = add(dag, 3, 1)
        sub_resp = sub(dag, 3, 1)
        assert add_resp.to_py() == 3 + 1
        assert sub_resp.to_py() == 3 - 1

class TestS3Executor(DmlTestBase):

    def test_upload_basic(self):
        dag = Dag.new(self.id())
        obj = b'this is a test'
        with pytest.raises(ValueError, match='s3_upload requires'):
            s3_upload(dag, obj, bucket=None, prefix='test')
        resource_node = s3_upload(dag, obj, bucket='daggerml-base', prefix='test')
        try:
            assert isinstance(resource_node, Node)
            resource = resource_node.to_py()
            assert isinstance(resource, S3Resource)
            assert resource.tag == 'com.daggerml.executor.s3'
            assert resource.uri.startswith('s3://daggerml-base/test/')
            assert resource.bucket == 'daggerml-base'
            assert resource.key.startswith('test/')
            resp = boto3.client('s3').get_object(
                Bucket=resource.bucket,
                Key=resource.key
            )
            assert resp['Body'].read() == obj
        finally:
            resp = boto3.client('s3').delete_object(
                Bucket=resource.bucket,
                Key=resource.key
            )
