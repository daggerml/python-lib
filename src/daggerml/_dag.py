import json
import logging
import traceback as tb
import warnings
from copy import copy, deepcopy
from dataclasses import dataclass
from time import sleep
from typing import NewType, Optional

import boto3
import requests
from requests_auth_aws_sigv4 import AWSSigV4

from daggerml._config import DML_API_ENDPOINT

logger = logging.getLogger(__name__)
conn_pool = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
conn_pool.mount('http://', adapter)
conn_pool.mount('https://', adapter)
boto3_session = boto3.session.Session()
api_region = None


def _json_dumps_default(obj):
    if isinstance(obj, Resource):
        return {'type': 'resource', 'value': obj.to_dict()}
    raise TypeError('unknown type %r' % type(obj))


def json_dumps(obj, *, skipkeys=False, ensure_ascii=True,
               check_circular=True, allow_nan=True,
               cls=None, indent=None, separators=(',', ':'),
               default=None, sort_keys=True, **kw):
    if default is not None:
        def _default(x):
            try:
                y = _json_dumps_default(x)
            except TypeError:
                y = default(x)
            return y
    else:
        def _default(x):
            return _json_dumps_default(x)
    return json.dumps(obj, skipkeys=skipkeys, ensure_ascii=ensure_ascii,
                      check_circular=check_circular, allow_nan=allow_nan,
                      cls=cls, indent=indent, separators=separators,
                      default=_default, sort_keys=sort_keys, **kw)


class DmlError(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg


class ApiError(DmlError):
    """raised if an API request fails"""
    pass


class DagError(DmlError):
    """raised when performing invalid operations on a dag"""
    pass


class NodeError(DmlError):
    """raised when a node fails"""
    pass


def _api(api, op, **kwargs):
    global api_region
    try:
        payload = dict(api=api, op=op, **kwargs)
        while True:
            assert DML_API_ENDPOINT is not None, 'API endpoint not configured'
            assert boto3_session.get_credentials() is not None, 'AWS credentials not found'
            url = DML_API_ENDPOINT + '/user'
            if api_region is None:
                resp = conn_pool.get(url)
                if resp.status_code != 200:
                    break
                api_region = resp.json()['region']
            auth = AWSSigV4('execute-api', region=api_region)
            resp = conn_pool.post(url, auth=auth, json=payload)
            data = resp.json()
            if resp.status_code != 504:
                break
        if resp.status_code != 200:
            raise ApiError(f'{resp.status_code} {resp.reason}')
        if data['status'] != 'ok':
            err = data['error']
            if err['context']:
                logger.error('api error: %s', err['message'] + '\n' + '\n'.join(err['context']))
            raise ApiError(f'{err["code"]}: {err["message"]}')
        return data['result']
    except KeyboardInterrupt:
        raise
    except ApiError:
        raise
    except Exception as e:
        raise ApiError(f'{e.__class__.__name__}: {str(e)}') from e


def list_dags(name=None):
    """list all completed dags

    Parameters
    ----------
    name : str
        name of the dag to list (will list all versions)
    """
    return _api('dag', 'list', name=name)


def describe_dag(dag_id):
    """describe a dag with its ID"""
    return _api('dag', 'describe', dag_id=dag_id)


def delete_dag(dag_id):
    return _api('dag', 'delete_dag', dag_id=dag_id)


def get_dag_by_name_version(dag_name, version='latest'):
    tmp = _api('dag', 'get_dag_by_name_version', name=dag_name, version=version)
    if tmp is None:
        return None
    return tmp['result']


def get_deleted_resources(dag_id, secret, exclusive_start_id=-1):
    return _api('dag', 'get_deleted_resources', dag_id=dag_id, secret=secret,
                exclusive_start_id=exclusive_start_id)


def get_node(node_id, secret):
    return _api('node', 'get_node', node_id=node_id, secret=secret)


def get_node_metadata(node_id, secret):
    return _api('node', 'get_node_metadata', node_id=node_id, secret=secret)


def format_exception(err):
    if isinstance(err, NodeError):
        return err.msg
    return {
        'message': str(err),
        'trace': tb.format_exception(type(err), value=err, tb=err.__traceback__)
    }


def daggerml():
    from collections.abc import Mapping
    from weakref import WeakKeyDictionary

    tag2resource = {}

    @dataclass(frozen=True)
    class Resource:
        """daggerml's datatype extension class"""
        id: str
        parent: Optional[NewType('Resource', None)]
        tag: Optional[str] = None

        @staticmethod
        def from_dict(data):
            if data['parent'] is None:
                parent = None
                cls = Resource
            else:
                parent = Resource.from_dict(data['parent'])
                cls = tag2resource.get(parent.tag, Resource)
            return cls(data['id'], parent, data['tag'])

        def to_dict(self):
            parent = self.parent
            if parent is not None:
                parent = parent.to_dict()
            return {'id': self.id, 'tag': self.tag, 'parent': parent}

    def register_tag(tag, cls=None):
        """register a tag with daggerml

        Once registered, any resources loaded with this tag will be of type: cls

        Parameters
        ----------
        tag : str
            the unique tag to register
        cls : Resource subclass
            the class representation of the resource
        """
        def wrapped(cls):
            assert issubclass(cls, Resource), 'invalid class: expected Resource'
            assert isinstance(tag, str), 'invalid tag: expected string'
            if tag in tag2resource:
                warnings.warn('tag already registered: ' + tag, stacklevel=2)
            tag2resource[tag] = cls
            return cls
        return wrapped if cls is None else wrapped(cls)

    def to_data(py):
        if isinstance(py, Node):
            return {'type': 'ref', 'value': {'node_id': py.id}}
        elif isinstance(py, (list, tuple)):
            return {'type': 'list', 'value': [to_data(x) for x in py]}
        elif isinstance(py, (dict, Mapping)):
            if not all([isinstance(x, str) for x in py]):
                raise TypeError('map datum keys must be strings')
            return {'type': 'map', 'value': {k: to_data(v) for (k, v) in py.items()}}
        elif isinstance(py, type(None)):
            return {'type': 'scalar', 'value': {'type': 'null'}}
        elif isinstance(py, str):
            return {'type': 'scalar', 'value': {'type': 'string', 'value': str(py)}}
        elif isinstance(py, bool):
            return {'type': 'scalar', 'value': {'type': 'boolean', 'value': bool(py)}}
        elif isinstance(py, int):
            return {'type': 'scalar', 'value': {'type': 'int', 'value': str(py)}}
        elif isinstance(py, float):
            return {'type': 'scalar', 'value': {'type': 'float', 'value': str(py)}}
        elif isinstance(py, Resource):
            return {'type': 'resource', 'value': py.to_dict()}
        else:
            raise ValueError('unknown type: ' + type(py))

    def from_data(res):
        t = res['type']
        v = res['value']
        if t == 'list':
            return tuple([from_data(x) for x in v])
        elif t == 'map':
            return {k: from_data(x) for (k, x) in v.items()}
        elif t == 'scalar':
            t = v['type']
            v = v.get('value')
            if t == 'boolean':
                return bool(v)
            elif t == 'int':
                return int(v)
            elif t == 'float':
                return float(v)
            elif t == 'string':
                return str(v)
            elif t == 'null':
                return None
            else:
                raise ValueError('unknown scalar type: ' + t)
        elif t == 'resource':
            return Resource.from_dict(v)
        else:
            raise ValueError('unknown type: ' + t)

    CACHE = WeakKeyDictionary()

    @dataclass(frozen=True)
    class Node:
        dag: NewType("Dag", None)
        id: str

        def __len__(self):
            if '$length' in self.meta:
                return self.meta['$length']
            keys = self.meta.get('$keys')
            if keys is not None:
                return len(keys)
            raise ValueError('cannot iterate of this node type')

        def __iter__(self):
            if self.meta['$type'] == 'list':
                for i in range(self.meta['$length']):
                    yield self[i]
            elif self.meta['$type'] == 'map':
                for key in self.meta['$keys']:
                    yield key
            else:
                raise ValueError('cannot iterate of this node type')

        def __add__(self, other):
            if len(other) == 0:
                return self
            f = self.dag.from_py([self.dag.db_executor, 'concat', self, other])
            resp = f()
            return resp

        def __getitem__(self, key):
            f = self.dag.from_py([self.dag.db_executor, 'get', self, key])
            resp = f()
            if self in CACHE:
                if isinstance(key, Node):
                    key = key.to_py()
                CACHE[resp] = CACHE[self][key]
            return resp

        def call_async(self, *args, **meta):
            """call a remote function asynchronously

            Parameters
            ----------
            *args : dml types
            **meta : json serializable

            Returns
            -------
            NodeWaiter
            """
            expr = self + args
            waiter = NodeWaiter(self.dag, expr.id, meta)
            return waiter

        def __call__(self, *args, **meta):
            """call a remote function

            Parameters
            ----------
            *args : dml types
            **meta : json serializable

            Returns
            -------
            Node
            """
            expr = self + args
            waiter = NodeWaiter(self.dag, expr.id, meta)
            waiter.wait(2)
            return waiter.result

        def to_py(self):
            """convert a node to python datastructures

            recursively pulls data from daggerml if its not already in the cache.
            """
            return self.dag.to_py(self)

        def __repr__(self):
            return f'Node({self.dag.name},{self.dag.version},{self.id})'

        @property
        def meta(self):
            resp = get_node_metadata(self.id, self.dag.secret)
            return resp

    @dataclass
    class NodeWaiter:
        def __init__(self, dag, expr, meta):
            self.dag = dag
            self.expr = expr
            self.meta = meta
            self._result = None
            self._resp = {}
            self.check()

        def __hash__(self):  # required for this to be a key in a map
            return hash(self.id)

        def check(self):
            self._resp = _api('dag', 'put_fnapp',
                              dag_id=self.dag.id,
                              expr=self.expr,
                              meta=self.meta,
                              token=self._resp.get('token'),
                              secret=self.dag.secret)
            return self.result

        @property
        def id(self):
            return self._resp['node_id']

        @property
        def result(self):
            if self._resp['success']:
                return Node(self.dag, self.id)
            if self._resp['error'] is not None:
                raise NodeError(self._resp['error'])
            return

        def wait(self, dt=0.1):
            while self.result is None:
                sleep(dt)
                self.check()
            return self.result

    @dataclass(frozen=True)
    class Dag:
        id: str
        name: str = None
        version: int = None
        expr_id: str = None
        db_exec: str = None
        # get_fn: str = None
        executor_id: str = None
        secret: str = None

        @classmethod
        def new(cls, name, version=None):
            """create a new dag"""
            resp = _api('dag', 'create_dag', name=name, version=version)
            if resp is not None:
                return cls(**resp)

        @classmethod
        def from_claim(cls, executor, secret, ttl, node_id=None):
            """claim a remote execution

            Parameters
            ----------
            executor : Resource
                the executor's resource
            secret : str
                the executor's secret
            ttl : int
                how long for the claim to be active for (before needing to refresh)
            node_id : str, optional
                the specific node_id to claim
            """
            if isinstance(executor, Resource):
                executor = executor.to_dict()
            resp = _api('node', 'claim_node',
                        executor=executor,
                        ttl=ttl,
                        node_id=node_id,
                        secret=secret)
            if resp is None:
                return
            return cls(**resp)

        @property
        def expr(self):
            """remote execution's expression"""
            return Node(self, self.expr_id)

        @property
        def executor(self):
            """this dag's executor resource"""
            return Node(self, self.executor_id).to_py()

        @property
        def db_executor(self):
            return Node(self, self.db_exec).to_py()

        def from_py(self, py):
            """convert a python datastructure to a literal node"""
            res = _api('dag', 'put_literal',
                       dag_id=self.id,
                       data=to_data(py),
                       secret=self.secret)
            node = Node(self, res['node_id'])
            if node not in CACHE:
                CACHE[node] = deepcopy(py)
            return node

        def to_py(self, node):
            """convert a [collection of] nodes to a python datastructure"""
            if isinstance(node, Node):
                if node.dag != self:
                    raise ValueError('node does not belong to dag')
                if node in CACHE:
                    py = self.to_py(CACHE[node])
                else:
                    py = from_data(get_node(node.id, self.secret))
                    CACHE[node] = deepcopy(py)
            elif isinstance(node, (tuple, list)):
                py = tuple([self.to_py(x) for x in node])
            elif isinstance(node, Mapping):
                py = {k: self.to_py(v) for k, v in node.items()}
            elif isinstance(node, (bool, str, int, float, type(None), Resource)):
                py = node
            return copy(py)

        def fail(self, failure_info={}):  # noqa: B006
            """fail a dag"""
            if isinstance(failure_info, (dict, list, tuple, Resource)):
                failure_info = json.loads(json_dumps(failure_info))
            _api('dag', 'fail_dag',
                 dag_id=self.id,
                 secret=self.secret,
                 failure_info=failure_info)
            return

        def commit(self, result):
            """commit a dag result"""
            result = self.from_py(result)
            _api('dag', 'commit_dag',
                 dag_id=self.id,
                 result=result.id,
                 secret=self.secret)
            return

        def delete(self):
            """delete a dag (must be failed / committed first)"""
            delete_dag(dag_id=self.id)
            return

        def refresh(self, ttl=300):
            """refresh a dag claim (ttl is same as in `from_claim`)"""
            res = _api('dag', 'refresh_claim', dag_id=self.id,
                       secret=self.secret, ttl=ttl,
                       refresh_token=self.id)
            if res is None:
                raise DagError('Failed to refresh dag!')
            return res

        def load(self, dag_name, version='latest'):
            """load a result from another dag"""
            node_id = get_dag_by_name_version(dag_name, version)
            if node_id is None:
                raise DagError('No such dag/version: %s / %r' % (dag_name, version))
            res = _api('dag', 'put_load', dag_id=self.id,
                       node_id=node_id, secret=self.secret)
            return Node(self, res['node_id'])

        def __repr__(self):
            return f'Dag({self.name},{self.version})'

        def __enter__(self):
            return self

        def __exit__(self, _, exc_val, __):
            if exc_val is not None:
                self.fail(format_exception(exc_val))
                logger.exception('failing dag')
                return True  # FIXME remove this to not catch these errors

    return Dag, Node, Resource, register_tag


Dag, Node, Resource, register_tag = daggerml()
del daggerml


def fullname(o):
    klass = o.__class__
    return klass.__module__ + '.' + klass.__qualname__


def dag_fn(fn):
    def wrapped(dag, *args, **kwargs):
        node = dag.from_py([dag.executor, fullname(fn)]).call_async(*args, **kwargs)
        while True:
            sleep(0.1)
            fn_dag = Dag.from_claim(dag.executor, dag.secret, ttl=-1, node_id=node.id)
            if isinstance(fn_dag, Dag):
                break
        with fn_dag:
            fn_dag.commit(fn(fn_dag))
        return node.wait()
    return wrapped
