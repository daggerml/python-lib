import hashlib
import json
import logging
import os
import traceback as tb
import warnings
from copy import copy, deepcopy
from dataclasses import dataclass
from functools import singledispatch
from io import BytesIO
from pathlib import Path
from time import sleep
from typing import IO, NewType, Optional, Union
from urllib.parse import urlparse

import boto3
import requests
from requests_auth_aws_sigv4 import AWSSigV4

from daggerml._config import DML_API_ENDPOINT, DML_S3_BUCKET, DML_S3_PREFIX

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


def get_dag_topology(dag_id):
    return _api('dag', 'get_topology', dag_id=dag_id)


def describe_node(node_id):
    """describe a dag with its ID"""
    return _api('node', 'describe', node_id=node_id)


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
            parent = data['parent']
            if parent is not None:
                parent = Resource.from_dict(parent)
            cls = tag2resource.get(data['tag'], Resource)
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
        if cls is None:
            return lambda x: register_tag(tag=tag, cls=x)
        assert issubclass(cls, Resource), 'invalid class: expected Resource subclass'
        assert isinstance(tag, str), 'invalid tag: expected string'
        if tag in tag2resource:
            warnings.warn('tag already registered: ' + tag, stacklevel=2)
        tag2resource[tag] = cls
        return cls

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

    CACHE = WeakKeyDictionary()

    def _run_dag_builtin(dag, builtin_op, **kw):
        # TODO: add try catch and maybe return a node error here?
        res = _api('dag', 'builtin', builtin_op=builtin_op, dag_id=dag.id, secret=dag.secret, **kw)
        if res['error'] is not None:
            raise NodeError(res['error']['message'])
        return Node(dag, res['node_id'])

    def _from_py(dag, py, **meta):
        """convert a python datastructure to a literal node"""
        if isinstance(py, (list, tuple)):
            items = [_from_py(dag, i).id for i in py]
            node = _run_dag_builtin(dag, 'construct_list', args=items, meta=meta)
        elif isinstance(py, (dict, Mapping)):
            items = {k: _from_py(dag, v).id for k, v in py.items()}
            node = _run_dag_builtin(dag, 'construct_map', args=items, meta=meta)
        else:
            res = _api('dag', 'put_literal',
                       dag_id=dag.id,
                       data=to_data(py),
                       secret=dag.secret)
            node = Node(dag, res['node_id'])
        if node not in CACHE:
            CACHE[node] = deepcopy(py)
        return node

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
            return _run_dag_builtin(self.dag, 'concat', args=[self.id, self.dag.from_py(other).id], meta={})

        def __getitem__(self, key):
            res = _run_dag_builtin(self.dag, 'get', args=[self.id, self.dag.from_py(key).id], meta={})
            if self in CACHE:
                if isinstance(key, Node):
                    key = key.to_py()
                CACHE[res] = CACHE[self][key]
            return res

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
        executor_id: str = None
        secret: str = None

        @classmethod
        def new(cls, name, version=None):
            """create a new dag"""
            resp = _api('dag', 'create_dag', name=name, version=version)
            if resp is not None:
                dag = cls(**resp)
                return dag

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

        def from_py(self, py, **meta):
            """convert a python datastructure to a literal node"""
            return _from_py(self, py, **meta)

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


def fullname(obj):
    return '.'.join([obj.__module__, obj.__qualname__])


def dag_fn(fn):
    """wraps a function into a dag node with executor being the dag's executor

    This is only reproducible if it's in the cloud. But that's okay.
    """
    def wrapped(dag, *args, **meta):
        node = dag.from_py([dag.executor, fullname(fn)]).call_async(*args, **meta)
        while True:
            fn_dag = Dag.from_claim(dag.executor, dag.secret, ttl=-1, node_id=node.id)
            if isinstance(fn_dag, Dag):
                break
            sleep(0.1)
        with fn_dag:
            fn_dag.commit(fn(fn_dag))
        return node.wait()
    return wrapped


@singledispatch
def hash_object(file_obj: IO, chunk_size: int = 8 * 1024 * 1024) -> str:
    """compute hash consistent with aws s3 etag of file"""
    # unconfirmed from https://stackoverflow.com/a/43819225
    md5s = []
    while data := file_obj.read(chunk_size):
        if data is None:
            break
        md5s.append(hashlib.md5(data))
    if len(md5s) < 1:
        return hashlib.md5().hexdigest()
    if len(md5s) == 1:
        return md5s[0].hexdigest()
    digests = b''.join(m.digest() for m in md5s)
    digests_md5 = hashlib.md5(digests)
    return '{}-{}'.format(digests_md5.hexdigest(), len(md5s))  # wrap this in double quotes for s3 etag


@hash_object.register
def _(file_obj: bytes, chunk_size: int = 8 * 1024 * 1024) -> str:
    """compute hash consistent with aws s3 etag of file"""
    return hash_object(BytesIO(file_obj), chunk_size)


@hash_object.register(str)
@hash_object.register(os.PathLike)
def _(file_obj: Union[str, os.PathLike], chunk_size: int = 8 * 1024 * 1024) -> str:
    """compute hash consistent with aws s3 etag of file"""
    with open(file_obj, 'rb') as fo:
        return hash_object(fo, chunk_size)


@register_tag('com.daggerml.executor.local')
@dataclass(frozen=True)
class LocalResource(Resource):
    """local files"""
    @property
    def js(self):
        return json.loads(self.id)

    @property
    def file_path(self):
        return self.js['path']

    @property
    def hash(self):
        return self.js['hash']

    @classmethod
    def from_file(cls, parent, file_path):
        """computes the hash of the file and incorporates that into the ID"""
        _id = json_dumps({'path': str(file_path), 'hash': hash_object(file_path)})
        return cls(id=_id, parent=parent, tag='com.daggerml.executor.local')


@register_tag('com.daggerml.executor.s3')
@dataclass(frozen=True)
class S3Resource(Resource):
    """data on s3"""
    @property
    def uri(self):
        return self.id

    @property
    def bucket(self):
        return urlparse(self.uri).netloc

    @property
    def key(self):
        return urlparse(self.uri).path[1:]

    @classmethod
    def from_uri(cls, parent, uri):
        return cls(id=uri, parent=parent, tag='com.daggerml.executor.s3')


@singledispatch
def s3_put_bytes_or_file(bytes_or_file: Union[IO, bytes], client, bucket, key):
    # FIXME check for the file first?
    return client.put_object(
        Body=bytes_or_file,
        Bucket=bucket,
        Key=key,
    )


@s3_put_bytes_or_file.register(str)
@s3_put_bytes_or_file.register(os.PathLike)
def _(bytes_or_file: Union[str, os.PathLike], client, bucket, key):
    with open(bytes_or_file, 'rb') as f:
        return s3_put_bytes_or_file(f, client, bucket, key)


def local_executor(dag, exec_name, exec_version):
    file_path = f'.dml/{exec_name}_{exec_version}.json'
    exec_dag = Dag.new(exec_name, exec_version)
    if exec_dag is not None:
        exec_dag.commit(exec_dag.executor)
        with open(file_path, 'w') as fo:
            json.dump(vars(exec_dag), fo)
        secret = exec_dag.secret
    else:
        with open(file_path, 'r') as fo:
            secret = json.load(fo)['secret']
    exec_node = dag.load(exec_name, version=exec_version)
    return exec_node.to_py(), secret


def delete_local_executor(exec_name, exec_version):
    file_path = f'.dml/{exec_name}_{exec_version}.json'
    with open(file_path, 'r') as fo:
        js = json.load(fo)
    delete_dag(js['id'])
    os.remove(file_path)
    return


local_executor.delete = delete_local_executor


def _s3_upload(dag, bytes_object, bucket, prefix, client):
    """upload data to s3"""
    if isinstance(bytes_object, LocalResource):
        bytes_object = str(Path(bytes_object.file_path).absolute())
    key = f'{prefix}/bytes/{hash_object(bytes_object)}'
    resource = S3Resource.from_uri(dag.executor, f's3://{bucket}/{key}')
    s3_put_bytes_or_file(bytes_object, client, bucket, key)
    return dag.from_py(resource)


def s3_upload(dag, bytes_object, exec_name=None, bucket=DML_S3_BUCKET, prefix=DML_S3_PREFIX, client=None):
    """upload data to s3"""
    if bucket is None:
        raise ValueError('`s3_upload requires `DML_S3_{BUCKET,PREFIX}` to be set')
    prefix = prefix.rstrip('/')
    if client is None:
        client = boto3.client('s3')
    if exec_name is None:
        return _s3_upload(dag, bytes_object, bucket, prefix, client)
    s3_exec, s3_secret = local_executor(dag, exec_name, 0)
    put_fn = dag.from_py([s3_exec]).call_async()
    with Dag.from_claim(s3_exec, s3_secret, 1, node_id=put_fn.id) as s3_dag:
        s3_dag.commit(_s3_upload(s3_dag, bytes_object, bucket, prefix, client))
    resp = put_fn.wait()
    return resp
