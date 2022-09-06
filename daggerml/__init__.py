import json
import weakref
import logging
from copy import copy
from time import sleep
from uuid import uuid4
from hashlib import md5
from warnings import warn
from typing import Callable, Any
from dataclasses import dataclass
from collections.abc import Mapping
from daggerml.util import api, tar, upload_file  # noqa: F401
from daggerml._types import Func, Resource
from daggerml.__about__ import __version__  # noqa: F401
from daggerml.exceptions import NodeError, DatumError


logger = logging.getLogger(__name__)


def init():
    """init function that returns stateful dag"""

    ###########################################################################
    # INDEXES
    ###########################################################################

    PROXY_FACTORY_BY_PY_TYPE = {}
    PROXY_FACTORY_BY_DB_TYPE = {}

    DATUM_BY_ID = weakref.WeakValueDictionary()
    PROXY_BY_DATUM = weakref.WeakKeyDictionary()

    ###########################################################################
    # SHARED VARS
    ###########################################################################

    DAG_ID = []
    # BUILTINS = {}
    STACK = []

    ###########################################################################
    # HELPERS
    ###########################################################################

    def commit_node(node_id, token, result):
        result = to_db(from_py(result))
        datum_id = PROXY_BY_DATUM[result].id
        finalized = api(
            'commit_node',
            node_id=node_id,
            token=token,
            datum_id=datum_id
        )['finalized']
        return finalized, result

    def upsert_and_claim(func_id, args):
        return api(
            'upsert_node_and_claim',
            func=func_id,
            dag_id=DAG_ID[0],
            args=args,
            ttl=0
        )

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

    class LocalFuncDatum(BaseDatum):
        def __call__(self, *args):
            args = [from_py(x) for x in args]
            proxy = PROXY_BY_DATUM[self]
            if proxy.dag_id != DAG_ID[0]:
                self = from_py(proxy.py)
                proxy = PROXY_BY_DATUM[self]
            if proxy.dag_id != DAG_ID[0]:
                raise ValueError('%s != %s' % (proxy.dag_id, DAG_ID[0]))
            [to_db(x) for x in [self, *args]]
            while True:
                claim = upsert_and_claim(proxy.id,
                                         [PROXY_BY_DATUM[x].id for x in args])
                token = claim['refresh_token']
                if claim['error'] is not None:
                    raise NodeError(claim['error'])
                if claim['result'] is not None:
                    return from_db(claim['result'])
                if token is not None:
                    break
                sleep(1)
            node_id = claim['node_id']
            STACK.append(node_id)
            try:
                result = proxy.py(*args)
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
                raise NodeError(str(e)) from e
            finally:
                STACK.pop()

    class FuncDatum(BaseDatum):
        def __call__(self, *args):
            args = [from_py(x) for x in args]
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
                    raise NodeError(json.dumps(node_info['error']))
                if result_id is not None:
                    return from_db(result_id)
                sleep(1)

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
            return proxy_type
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
            self.members = [from_py(x) for x in py]
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
            self.members = {k: from_py(v) for (k, v) in py.items()}
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
                name = name + uuid4().hex
            self.py = py
            if len(DAG_ID) > 0:
                self.members = from_py(f'{DAG_ID[0]}/{name}')
                self.dag_id = copy(DAG_ID[0])
            else:
                self.members = from_py(name)
                self.dag_id = ''
            return {'executor': 'local', 'func_datum': PROXY_BY_DATUM[self.members].id}

        def data2py(self, data):
            return self.py

        def persist_members(self):
            to_db(self.members)

    @proxyclass(FuncDatum, 'func', Func)
    class FuncProxy(BaseProxy):
        def py2data(self, py):
            self.members = from_py(py.data)
            return {'executor': py.executor, 'func_datum': PROXY_BY_DATUM[self.members].id}

        def data2py(self, data):
            return Func(to_py(data['executor']), to_py(from_db(self.value['data']['func_datum'])))

        def persist_members(self):
            to_db(self.members)

    @proxyclass(ScalarDatum, 'resource', Resource)
    class ResourceProxy(BaseProxy):
        def py2data(self, py):
            return {'type': py.type, 'data': py.data}

        def data2py(self, data):
            return Resource(data['type'], data['data'])

    ###########################################################################
    # DB DATUM STORE
    ###########################################################################

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
        proxy = PROXY_BY_DATUM[datum]
        if not proxy.persisted:
            proxy.persist_members()
            id = api('upsert_datum', value=proxy.value)['id']
            assert id == proxy.id, 'proxy ID (%s) != DB ID (%s)' % (proxy.id, id)
            proxy.persisted = True
        return datum

    ###########################################################################
    # MODULE API FUNCTIONS
    ###########################################################################

    def from_py(datum_or_py):
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

    def func(f):
        return from_py(f)

    def load(dag_name, version=None):
        datum = from_json(api(
            'get_dag_result',
            dag_name=dag_name,
            version=version
        ))
        if len(DAG_ID) == 0:
            warn('loading data from outside of a dag will impare tracing')
            return datum.to_py()
        return datum

    def to_py(datum_or_py):
        if not isinstance(datum_or_py, BaseDatum):
            return datum_or_py
        return PROXY_BY_DATUM[datum_or_py].to_py()

    def run(f, *args, name):
        if len(DAG_ID) > 0:
            # FIXME need to have lock dags too so that we can finish them one
            # way or another.
            raise RuntimeError('cannot start a new dag while one is already running...')
        DAG_ID.append(api('create_dag', name=name)['id'])
        try:
            result = func(f)(*args)
            api('commit_dag', dag_id=DAG_ID[0], datum_id=PROXY_BY_DATUM[result].id)
            return result
        except Exception:
            logger.exception('exception in running dag')
            api('fail_dag', dag_id=DAG_ID[0])
            raise
        finally:
            DAG_ID.pop()
            # PROXY_FACTORY_BY_PY_TYPE.clear()
            # PROXY_FACTORY_BY_DB_TYPE.clear()
            # DATUM_BY_ID.clear()
            # PROXY_BY_DATUM.clear()
        return

    return Dag(func, run, load, to_py)


@dataclass(frozen=True)
class Dag:
    """A stateful dag

    This is the entrypoint for standard DML stuff

    Examples
    --------
    >>> dag = init()
    >>> @dag.func
    ... def inc(x):
    ...     return x.to_py() + 1
    >>> def main(n):
    ...     return [inc(x) for x in range(n.to_py())]
    >>> dag.run(main, 5, name='test').to_py()
    [1, 2, 3, 4, 5]
    """
    _func: Callable[[Callable], Callable]
    _run: Callable[[Callable], Any]
    _load: Callable[[str], Any]
    _to_py: Callable[[Any], Any]

    def func(self, f):
        """decorator to turn a function into a daggerml func

        Parameters
        ----------
        f : callable

        Returns
        -------
        A lazy daggerml tracked function
        """
        return self._func(f)

    def run(self, f, *args, name):
        """run a dag

        Parameters
        ----------
        f : callable
            this should be the main function you want to run
        args : Sequence[Any]
            These will be passed to the func

        Returns
        -------
        The result of `f(*args)` but as a datum
        """
        return self._run(f, *args, name=name)

    def load(self, dag_name, version=None):
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
        return self._load(dag_name, version)

    def to_py(self, datum_or_py):
        """convert a Datum to its corresponding python object

        Alternatively, you could run `datum.to_py()`
        """
        return self._to_py(datum_or_py)
