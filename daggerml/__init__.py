from daggerml.__about__ import __version__  # noqa: F401


def daggerml():
    import json
    import logging
    from time import sleep
    from collections.abc import Mapping
    from weakref import WeakKeyDictionary
    from daggerml._config import DML_API_ENDPOINT
    from dataclasses import dataclass, field
    from http.client import HTTPConnection, HTTPSConnection
    from typing import Any, NewType
    from urllib.parse import urlparse
    from uuid import uuid4

    logger = logging.getLogger(__name__)

    class DmlError(Exception):
        pass

    class ApiError(DmlError):
        pass

    class DatumError(DmlError):
        pass

    class NodeError(DmlError):
        def __init__(self, message):
            self.message = message

        def __str__(self):
            return 'NodeError: ' + self.message

    @dataclass(frozen=True)
    class Resource:
        type: str
        data: Any

    def api(op, **kwargs):
        try:
            url = urlparse(DML_API_ENDPOINT)
            scheme = url.scheme or 'http'
            host = url.hostname or 'localhost'
            port = url.port or 80
            path = url.path or '/'
            conn = (HTTPConnection if scheme == 'http' else HTTPSConnection)(host, port)
            headers = {'content-type': 'application/json', 'accept': 'application/json'}
            conn.request('POST', path, json.dumps(dict(op=op, **kwargs)), headers)
            resp = conn.getresponse()
            if resp.status != 200:
                raise ApiError(f'{resp.status} {resp.reason}')
            resp = json.loads(resp.read())
            if resp['status'] != 'ok':
                err = resp['error']
                if err['context']:
                    logger.error('api error: %s', '\n'.join(err['context']))
                raise ApiError(f'{err["code"]}: {err["message"]}')
            return resp['result']
        except KeyboardInterrupt:
            raise
        except ApiError:
            raise
        except Exception as e:
            raise ApiError(f'{e.__class__.__name__}: {str(e)}')

    def to_data(py):
        if isinstance(py, list) or isinstance(py, tuple):
            return {'type': 'list', 'value': [to_data(x) for x in py]}
        elif isinstance(py, dict) or isinstance(py, Mapping):
            if not all([isinstance(x, str) for x in py]):
                raise TypeError('map datum keys must be strings')
            return {'type': 'map', 'value': {k: to_data(v) for (k, v) in py.items()}}
        elif isinstance(py, type(None)):
            return {'type': 'scalar', 'value': {'type': 'null'}}
        elif isinstance(py, str):
            return {'type': 'scalar', 'value': {'type': 'string', 'value': str(py)}}
        elif isinstance(py, int):
            return {'type': 'scalar', 'value': {'type': 'int', 'value': str(py)}}
        elif isinstance(py, float):
            return {'type': 'scalar', 'value': {'type': 'float', 'value': str(py)}}
        else:
            raise ValueError('unknown type: ' + type(py))

    def from_data(res):
        t = res['type']
        v = res['value']
        if t == 'list':
            return [from_data(x) for x in v]
        elif t == 'map':
            return {k: from_data(x) for (k, x) in v.items()}
        elif t == 'scalar':
            t = v['type']
            v = v.get('value')
            if t == 'int':
                return int(v)
            elif t == 'float':
                return float(v)
            elif t == 'string':
                return str(v)
            elif t == 'null':
                return None
            else:
                raise ValueError('unknown scalar type: ' + t)
        else:
            raise ValueError('unknown type: ' + t)

    CACHE = WeakKeyDictionary()

    @dataclass(frozen=True)
    class Node:
        dag: NewType("Dag", None)
        id: str

        def __len__(self):
            return len(self.dag.to_py(self))

        def __getitem__(self, key):
            f = self.dag.from_py(['db-executor', 'get'])
            return f(self, key)

        def __call__(self, *args):
            args = [self.dag.from_py(x) for x in args]
            if callable(CACHE.get(self)):
                resp = api('put_node_and_claim', dag_id=self.dag.id, ttl=0,
                           expr=[self.id] + [x.id for x in args])
                if resp['success']:
                    return Node(self.dag, resp['node_id'])
                if resp['error'] is not None:
                    logger.debug('ignoring error: %s', json.dumps(resp['error']))
                try:
                    result = CACHE[self](*args)
                    resp2 = api('commit_node', node_id=resp['node_id'],
                                token=resp['refresh_token'], data=to_data(result))
                    assert resp2['finalized'], 'failed to finalize node'
                except Exception as e:
                    api('fail_node', node_id=resp['node_id'], token=resp['refresh_token'],
                        error={'message': str(e)})
                    raise NodeError(str(e))
                n = Node(self.dag, resp['node_id'])
                CACHE[n] = result
                return n
            while True:
                resp = api('put_node', dag_id=self.dag.id,
                           expr=[self.id] + [x.id for x in args])
                if resp['success']:
                    return Node(self.dag, resp['node_id'])
                if resp['error'] is not None:
                    logger.debug('ignoring error: %s', json.dumps(resp['error']))
                    raise NodeError(json.dumps(resp['error']))
                sleep(10)
            return

        def to_py(self):
            return self.dag.to_py(self)

    @dataclass(frozen=True)
    class Dag:
        """
        This is the docs for the Dag class.
        """
        name: str
        id: str = field(init=False)
        version: int = field(init=False)

        def __post_init__(self):
            if not hasattr(self, 'id'):
                res = api('create_dag', name=self.name)
                object.__setattr__(self, 'id', res['id'])
                object.__setattr__(self, 'version', res['version'])

        def from_py(self, py):
            if isinstance(py, Node):
                return py
            if callable(py):
                fn_id = py.__qualname__
                if fn_id == '<lambda>':
                    fn_id += uuid4().hex
                fn_id = self.id + '/' + py.__module__ + ':' + fn_id
                res = api('put_literal_node', dag_id=self.id,
                          data=to_data(['local-executor', fn_id]))
                node = Node(self, res['node_id'])
            else:
                res = api('put_literal_node', dag_id=self.id, data=to_data(py))
                node = Node(self, res['node_id'])
            if node not in CACHE:
                CACHE[node] = py
            return node

        def to_py(self, node):
            if node.dag != self:
                raise ValueError('node does not belong to dag')
            if node in CACHE:
                return CACHE[node]
            py = from_data(api('get_node', node_id=node.id))
            CACHE[node] = py
            return py

    return DmlError, ApiError, DatumError, NodeError, Resource, Dag, Node


(DmlError, ApiError, DatumError, NodeError, Resource, Dag, Node) = daggerml()
del daggerml
