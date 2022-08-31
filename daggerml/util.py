import os
import json
import weakref
from time import sleep
from uuid import uuid4
from typing import Any
from hashlib import md5
from dataclasses import dataclass
from collections.abc import Mapping
from http.client import HTTPConnection, HTTPSConnection

try:
    import boto3
except ImportError:
    boto3 = None


class DmlError(Exception):
    pass


class ApiError(DmlError):
    pass


class DatumError(DmlError):
    pass


class FailedNodeException(DmlError):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'FailedNodeException: ' + self.message


def setup():

    ###########################################################################
    # AWS
    ###########################################################################

    DML_ZONE = os.getenv('DML_ZONE', 'prod')
    DML_GID = os.getenv('DML_GID', 'test-A')
    AWS_REGION = os.getenv('DML_REGION', 'us-west-2')

    ###########################################################################
    # INDEXES
    ###########################################################################

    PROXY_FACTORY_BY_PY_TYPE = {}
    PROXY_FACTORY_BY_DB_TYPE = {}

    DATUM_BY_ID = weakref.WeakValueDictionary()
    PROXY_BY_DATUM = weakref.WeakKeyDictionary()

    ###########################################################################
    # HELPERS
    ###########################################################################

    def api(op, **kwargs):
        try:
            if 'DML_LOCAL_DB' in os.environ:
                conn = HTTPConnection("localhost", 8000)
            else:
                conn = HTTPSConnection(f"api.{DML_ZONE}-{AWS_REGION}.daggerml.com")
            if 'gid' not in kwargs:
                kwargs['gid'] = DML_GID
            headers = {'content-type': 'application/json', 'accept': 'application/json'}
            conn.request('POST', '/', json.dumps(dict(op=op, **kwargs)), headers)
            resp = conn.getresponse()
            if resp.status != 200:
                raise ApiError(f'{resp.status} {resp.reason}')
            resp = json.loads(resp.read())
            if resp['status'] != 'ok':
                err = resp['error']
                if err['context']:
                    print('\n'.join(err['context']))
                raise ApiError(f'{err["code"]}: {err["message"]}')
            return resp['result']
        except KeyboardInterrupt:
            raise
        except ApiError:
            raise
        except Exception as e:
            raise ApiError(f'{e.__class__.__name__}: {str(e)}')

    ###########################################################################
    # SHARED VARS
    ###########################################################################

    DAG_ID = []
    # BUILTINS = {}
    STACK = [None]

    ###########################################################################
    # DATUM
    ###########################################################################

    class BaseDatum:
        def __repr__(self):
            name = self.__class__.__name__
            json = PROXY_BY_DATUM[self].to_json()
            return f'{name}({json})'

        def to_py(self):
            return to_py(self)

    class ScalarDatum(BaseDatum):
        pass

    class CollectionDatum(BaseDatum):
        def __len__(self):
            return len(PROXY_BY_DATUM[self].value['data'])

        def __getitem__(self, key):
            return from_db(PROXY_BY_DATUM[self].value['data'][key])

    class ListDatum(CollectionDatum):
        def __iter__(self):
            for x in PROXY_BY_DATUM[self].value['data']:
                yield from_db(x)

    class MapDatum(CollectionDatum):
        def __iter__(self):
            for x in PROXY_BY_DATUM[self].value['data']:
                yield x

        def keys(self):
            return PROXY_BY_DATUM[self].value['data'].keys()

    def commit_node(node_id, token, result):
        result = to_db(from_py_eager(result))
        datum_id = PROXY_BY_DATUM[result].id
        finalized = api(
            'commit_node',
            node_id=node_id,
            token=token,
            datum_id=datum_id
        )['finalized']
        return finalized, result

    class LocalFuncDatum(BaseDatum):
        def __call__(self, *args):
            args = [from_py_eager(x) for x in args]
            [to_db(x) for x in [self, *args]]
            proxy = PROXY_BY_DATUM[self]
            claim = api(
                'upsert_node_and_claim',
                func=proxy.id,
                dag_id=DAG_ID[0],
                args=[PROXY_BY_DATUM[x].id for x in args],
                ttl=0
            )
            result = claim['result']
            token = claim['refresh_token']
            node_id = claim['node_id']
            error = claim['error']
            if error is not None:
                raise FailedNodeException(error)
            if result is not None:
                return from_db(result)
            STACK.append(node_id)
            try:
                result = PROXY_BY_DATUM[self].py(*args)
                finalized, result = commit_node(node_id, token, result)
                assert finalized
                return result
            except Exception as e:
                api(
                    'fail_node',
                    node_id=node_id,
                    token=token,
                    error={'message': str(e)}
                )
                raise FailedNodeException(str(e)) from e
            finally:
                STACK.pop()

    class FuncDatum(BaseDatum):
        def __call__(self, *args):
            args = [from_py_eager(x) for x in args]
            [to_db(x) for x in [self, *args]]
            proxy = PROXY_BY_DATUM[self]
            while True:
                node_info = api(
                    'upsert_node',
                    func=proxy.id,
                    dag_id=DAG_ID[0],
                    args=[PROXY_BY_DATUM[x].id for x in args]
                )
                result_id = node_info['result']
                if node_info['error'] is not None:
                    raise FailedNodeException(json.dumps(node_info['error']))
                if result_id is not None:
                    return from_db(result_id)
                sleep(1)

    class LazyDatum:
        def __init__(self, thunk):
            self.thunk = thunk
            self.datum = None

        def force(self):
            if not len(DAG_ID):
                raise DatumError('lazy datum realized in module or global scope')
            if self.datum is None:
                self.datum = self.thunk()
            return self.datum

        def __len__(self):
            return self.force().__len__()

        def __getitem__(self, key):
            return LazyDatum(lambda: self.force().__getitem__(key))

        def __iter__(self):
            return self.force().__iter__()

        def keys(self):
            return LazyDatum(lambda: self.force().keys())

        def __call__(self, *args):
            return LazyDatum(lambda: self.force().__call__(*args))

        def to_py(self):
            return to_py(self)

    ###########################################################################
    # DATUM PROXY
    ###########################################################################

    def proxyclass(datum_type, db_type, *py_types):
        def ret(proxy_type):
            def factory():
                return proxy_type(datum_type, db_type)
            PROXY_FACTORY_BY_DB_TYPE[db_type] = factory
            for py_type in py_types:
                PROXY_FACTORY_BY_PY_TYPE[py_type] = factory
        return ret

    class BaseProxy:
        def __init__(self, datum_type, db_type):
            self.datum_type = datum_type
            self.db_type = db_type
            self.persisted = False
            self.id = self.value = self.members = None

        def from_py(self, py):
            self.value = {'type': self.db_type, 'data': self.py2data(py)}
            self.id = md5(json.dumps(self.value, sort_keys=True).encode()).hexdigest()
            return self

        def to_py(self):
            return self.data2py(self.value['data'])

        def from_json(self, json):
            self.id = json['id']
            self.value = json['value']
            self.persisted = True
            return self

        def to_json(self):
            return {'id': self.id, 'value': self.value}

        def py2data(self, py):
            return py

        def data2py(self, data):
            return data

        def persist_members(self):
            pass

    @proxyclass(ScalarDatum, 'null', type(None))
    class NullProxy(BaseProxy):
        pass

    @proxyclass(ScalarDatum, 'string', str)
    class StringProxy(BaseProxy):
        pass

    @proxyclass(ScalarDatum, 'int', int)
    class IntProxy(BaseProxy):
        pass

    @proxyclass(ScalarDatum, 'float', float)
    class FloatProxy(BaseProxy):
        pass

    @proxyclass(ListDatum, 'list', list, tuple, type(dict().keys()))
    class ListProxy(BaseProxy):
        def py2data(self, py):
            self.members = [from_py_eager(x) for x in py]
            return [PROXY_BY_DATUM[x].id for x in self.members]

        def data2py(self, data):
            if self.members is None:
                self.members = [from_db(x) for x in data]
            return [to_py(x) for x in self.members]

        def persist_members(self):
            if self.members is not None:
                [to_db(x) for x in self.members]

    @proxyclass(MapDatum, 'map', dict, Mapping)
    class MapProxy(BaseProxy):
        def py2data(self, py):
            if not all([isinstance(x, str) for x in py]):
                raise TypeError('map datum keys must be strings')
            self.members = {k: from_py_eager(v) for (k, v) in py.items()}
            return {k: PROXY_BY_DATUM[v].id for (k, v) in self.members.items()}

        def data2py(self, data):
            if self.members is None:
                self.members = {k: from_db(v) for (k, v) in data.items()}
            return {k: to_py(v) for (k, v) in self.members.items()}

        def persist_members(self):
            if self.members is not None:
                [to_db(self.members[k]) for k in self.members]

    @proxyclass(LocalFuncDatum, 'local-func', type(lambda x: x))
    class LocalFuncProxy(BaseProxy):
        def py2data(self, py):
            name = py.__qualname__
            if name == '<lambda>':
                name = name + str(uuid4()).replace('-', '')
            self.py = py
            self.members = from_py_eager(f'{DAG_ID[0]}/{name}')
            return {'executor': 'local', 'func_datum': PROXY_BY_DATUM[self.members].id}

        def data2py(self, data):
            return self.py

        def persist_members(self):
            to_db(self.members)

    @dataclass(frozen=True)
    class Func:
        executor: str
        data: Any

    @proxyclass(FuncDatum, 'func', Func)
    class FuncProxy(BaseProxy):
        def py2data(self, py):
            self.members = from_py_eager(py.data)
            return {'executor': py.executor, 'func_datum': PROXY_BY_DATUM[self.members].id}

        def data2py(self, data):
            return Func(to_py(data['executor']), to_py(from_db(self.value['data']['func_datum'])))

        def persist_members(self):
            to_db(self.members)

    @dataclass(frozen=True)
    class Resource:
        type: str
        data: Any

    @proxyclass(ScalarDatum, 'resource', Resource)
    class ResourceProxy(BaseProxy):
        def py2data(self, py):
            return {'type': py.type, 'data': py.data}

        def data2py(self, data):
            return Resource(data['type'], data['data'])

    ###########################################################################
    # DB DATUM STORE
    ###########################################################################

    def force(datum):
        if isinstance(datum, LazyDatum):
            datum = datum.force()
        return datum

    def from_json(json):
        if json is None:
            raise DatumError('not found')
        id = json['id']
        datum = DATUM_BY_ID.get(id)
        if datum is None:
            proxy = PROXY_FACTORY_BY_DB_TYPE[json['value']['type']]().from_json(json)
            datum = proxy.datum_type()
            DATUM_BY_ID[id] = datum
            PROXY_BY_DATUM[datum] = proxy
        return datum

    def from_db(datum_or_id):
        if isinstance(datum_or_id, BaseDatum):
            return datum_or_id
        datum = DATUM_BY_ID.get(datum_or_id)
        if datum is None:
            datum = from_json(api('get_datum', id=datum_or_id))
        return datum

    def to_db(datum):
        datum = force(datum)
        proxy = PROXY_BY_DATUM[datum]
        if not proxy.persisted:
            proxy.persist_members()
            id = api('upsert_datum', value=proxy.value)['id']
            assert id == proxy.id, 'proxy ID (%s) != DB ID (%s)' % (proxy.id, id)
            proxy.persisted = True
        return datum

    def from_py_eager(datum_or_py):
        datum_or_py = force(datum_or_py)
        if isinstance(datum_or_py, BaseDatum):
            return datum_or_py
        proxy = None
        for k, v in PROXY_FACTORY_BY_PY_TYPE.items():
            if isinstance(datum_or_py, k):
                proxy = v().from_py(datum_or_py)
                break
        if proxy is None:
            raise NotImplementedError(f'datum from type: {datum_or_py.__class__.__name__}')
        datum = DATUM_BY_ID.get(proxy.id)
        if datum is None:
            datum = proxy.datum_type()
            DATUM_BY_ID[proxy.id] = datum
            PROXY_BY_DATUM[datum] = proxy
        return datum

    ###########################################################################
    # MODULE API FUNCTIONS
    ###########################################################################

    def func(f):
        return LazyDatum(lambda: from_py_eager(f))

    def load(dag_name, version=None):
        return LazyDatum(lambda: from_json(api(
            'get_dag_result',
            dag_name=dag_name,
            version=version
        )))

    def from_py(datum_or_py):
        return LazyDatum(lambda: from_py_eager(datum_or_py))

    def to_py(datum_or_py):
        datum_or_py = force(datum_or_py)
        if not isinstance(datum_or_py, BaseDatum):
            return datum_or_py
        return PROXY_BY_DATUM[datum_or_py].to_py()

    def run(f, *args, name):
        DAG_ID.append(api('create_dag', name=name)['id'])
        # BUILTINS.update(from_json(api(
        #     'get_dag_result',
        #     dag_name='builtin',
        #     version=None
        # )))
        try:
            result = (func(f)(*args)).force()
            api('commit_dag', dag_id=DAG_ID[0], datum_id=PROXY_BY_DATUM[result].id)
            return result
        except Exception:
            api('fail_dag', dag_id=DAG_ID[0])
        return

    return Resource, Func, func, run, load, to_py, from_db, commit_node


Resource, Func, func, run, load, to_py, from_db, commit_node = setup()

del setup

Resource.__doc__ = \
    """an external resource (e.g. docker image, s3 object, etc.)

    Attributes
    ----------
    type : str
        the resource type (e.g. docker-image, or s3-blob, etc.)
    data : dict[str, any]
        the requisite data for accessing and permissioning the data (the
        s3-location, etc.).

    Notes
    -----
    You should only instantiate this class directly if you really know what
    you're doing. Otherwise, there are helper functions that return these
    things.
    """

run.__doc__ = \
    """run a dag

    Parameters
    ----------
    f : callable
        this should be the main function you want to run
    *args
        These will be passed to the func
    name : str
        the name of the dag. This will be versioned.

    Returns
    -------
    The result of f(*args) but as a datum
    """

load.__doc__ = \
    """load the result of a previously run dag

    Parameters
    ----------
    dag_name : str
        the name of the dag to load
    version : int

    Returns
    -------
    The result of that version of the dag run
    """

func.__doc__ = \
    """decorator to turn a function into a daggerml func

    Parameters
    ----------
    f : callable

    Returns
    -------
    A lazy daggerml tracked function
    """

Func.__doc__ = \
    """A python class representing a cloud function

    Attributes
    ----------
    executor : str
        the executor ID (what infrastructure does this func run on)
    data : any
        the data that defines the func (for that executor)
    """

to_py.__doc__ = \
    """convert a Datum or LazyDatum to its corresponding python object

    Alternatively, you could run `datum.to_py()`
    """

commit_node.__doc__ = \
    """commit a node. Only used in executors
    """
