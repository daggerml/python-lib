import json
import logging
import shutil
import subprocess
from dataclasses import dataclass, field, fields
from tempfile import TemporaryDirectory
from traceback import format_exception
from typing import Any, Callable, List, NewType, overload

from daggerml.util import current_time_millis, kwargs2opts, raise_ex

logger = logging.getLogger(__name__)

DATA_TYPE = {}

Node = NewType('Node', None)
Resource = NewType('Resource', None)
Error = NewType('Error', None)
Ref = NewType('Ref', None)
Dml = NewType('Dml', None)
Dag = NewType('Dag', None)
Scalar = str | int | float | bool | type(None) | Resource | Node
Collection = list | tuple | set | dict


def dml_type(cls=None):
    def decorator(cls):
        DATA_TYPE[cls.__name__] = cls
        return cls
    return decorator(cls) if cls else decorator


def from_data(data):
    n, *args = data if isinstance(data, list) else [None, data]
    if n is None:
        return args[0]
    if n == 'l':
        return [from_data(x) for x in args]
    if n == 's':
        return {from_data(x) for x in args}
    if n == 'd':
        return {k: from_data(v) for (k, v) in args}
    if n in DATA_TYPE:
        return DATA_TYPE[n](*[from_data(x) for x in args])
    raise ValueError(f'no decoder for type: {n}')


def to_data(obj):
    if isinstance(obj, Node):
        obj = obj.ref
    if isinstance(obj, tuple):
        obj = list(obj)
    n = obj.__class__.__name__
    if isinstance(obj, (type(None), str, bool, int, float)):
        return obj
    if isinstance(obj, (list, set)):
        return [n[0], *[to_data(x) for x in obj]]
    if isinstance(obj, dict):
        return [n[0], *[[k, to_data(v)] for k, v in obj.items()]]
    if n in DATA_TYPE:
        return [n, *[to_data(getattr(obj, x.name)) for x in fields(obj)]]
    raise ValueError(f'no encoder for type: {n}')


def from_json(text):
    return from_data(json.loads(text))


def to_json(obj):
    return json.dumps(to_data(obj), separators=(',', ':'))


@dml_type
@dataclass(frozen=True)
class Ref:  # noqa: F811
    to: str


@dml_type
@dataclass(frozen=True, slots=True)
class Resource:  # noqa: F811
    uri: str
    data: str | None = None
    adapter: str | None = None


@dml_type
@dataclass
class Error(Exception):  # noqa: F811
    message: str | Exception
    context: dict = field(default_factory=dict)
    code: str | None = None

    def __post_init__(self):
        if isinstance(self.message, Error):
            ex = self.message
            self.message = ex.message
            self.context = ex.context
            self.code = ex.code
        elif isinstance(self.message, Exception):
            ex = self.message
            self.message = str(ex)
            self.context = {'trace': format_exception(type(ex), value=ex, tb=ex.__traceback__)}
            self.code = type(ex).__name__
        else:
            self.code = type(self).__name__ if self.code is None else self.code

    def __str__(self):
        return ''.join(self.context.get('trace', [self.message]))


class Dml:  # noqa: F811
    def __init__(self, *, data=None, message_handler=None, **kwargs):
        self.data = data
        self.message_handler = message_handler
        self.kwargs = kwargs
        self.opts = kwargs2opts(**kwargs)
        self.token = None
        self.tmpdirs = None
        self.cache_key = None
        self.dag_dump = None

    def __call__(self, *args: str, as_text: bool = False) -> Any:
        resp = None
        path = shutil.which('dml')
        argv = [path, *self.opts, *args]
        resp = subprocess.run(argv, check=True, capture_output=True, text=True).stdout or ''
        try:
            resp = resp if as_text else json.loads(resp)
        except json.decoder.JSONDecodeError:
            pass
        return resp

    def __getattr__(self, name: str):
        def invoke(*args, **kwargs):
            return from_data(self('dag', 'invoke', self.token, to_json([name, args, kwargs])))
        return invoke

    def __enter__(self):
        self.tmpdirs = [TemporaryDirectory() for _ in range(2)]
        self.kwargs = {
            'config_dir': self.tmpdirs[0].__enter__(),
            'project_dir': self.tmpdirs[1].__enter__(),
            'repo': 'test',
            'user': 'test',
            'branch': 'main',
            **self.kwargs,
        }
        self.opts = kwargs2opts(**self.kwargs)
        self.cache_key, self.dag_dump = from_json(self.data or to_json([None, None]))
        if self.kwargs['repo'] not in [x['name'] for x in self('repo', 'list')]:
            self('repo', 'create', self.kwargs['repo'])
        if self.kwargs['branch'] not in self('branch', 'list'):
            self('branch', 'create', self.kwargs['branch'])
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        [x.__exit__(exc_type, exc_value, traceback) for x in self.tmpdirs]
        if exc_value and self.message_handler:
            self.message_handler(to_json(Error(exc_value)))

    def new(self, name: str, message: str) -> Dag:
        opts = [] if not self.dag_dump else kwargs2opts(dag_dump=self.dag_dump)
        self.token = self('dag', 'create', *opts, name, message, as_text=True)
        return Dag(self, self.token, self.dag_dump, self.message_handler)


@dataclass
class Dag:  # noqa: F811
    dml: Dml
    token: str
    dump: str | None = None
    message_handler: Callable | None = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value is not None:
            self.commit(Error(exc_value))
        if self.dump and self.message_handler:
            self.message_handler(self.dump)

    @property
    def expr(self) -> Node:
        ref = self.dml.get_expr()
        assert isinstance(ref, Ref)
        return Node(self, ref)

    def put(self, value: Scalar | Collection, *, name=None, doc=None) -> Node:
        assert not isinstance(value, Node) or value.dag == self
        return Node(self, self.dml.put_literal(value, name=name, doc=doc))

    def load(self, dag_name, *, name=None, doc=None) -> Node:
        return Node(self, self.dml.put_load(dag_name, name=name, doc=doc))

    def commit(self, value) -> Node:
        if isinstance(value, Error):
            pass
        value = value if isinstance(value, (Node, Error)) else self.put(value)
        self.dump = self.dml.commit(value)


@dataclass(frozen=True)
class Node:  # noqa: F811
    dag: Dag
    ref: Ref

    def __repr__(self):
        return f'{self.__class__.__name__}({self.ref.to})'

    def __hash__(self):
        return hash(self.ref)

    @overload
    def __getitem__(self, key: slice) -> List[Node]:
        ...

    @overload
    def __getitem__(self, key: str | int) -> Node:
        ...

    @overload
    def __getitem__(self, key: Node) -> Node:
        ...

    def __getitem__(self, key):
        if isinstance(key, slice):
            key = [key.start, key.stop, key.step]
        return Node(self.dag, self.dag.dml.get(self, key))

    def __len__(self):  # python requires this to be an int
        result = self.len().value()
        assert isinstance(result, int)
        return result

    def __iter__(self):
        if self.type().value() == 'list':
            for i in range(len(self)):
                yield self[i]
        elif self.type().value() == 'dict':
            for k in self.keys():
                yield k

    def __call__(self, *args, name=None, doc=None, timeout=30000) -> Node:
        args = [self.dag.put(x) for x in args]
        end = current_time_millis() + timeout
        while current_time_millis() < end:
            resp = raise_ex(self.dag.dml.start_fn([self, *args], name=name, doc=doc))
            if resp:
                return Node(self.dag, resp)
        raise TimeoutError(f'invoking function: {self.value()}')

    def keys(self, *, name=None, doc=None) -> Node:
        return Node(self.dag, self.dag.dml.keys(self, name=name, doc=doc))

    def len(self, *, name=None, doc=None) -> Node:
        return Node(self.dag, self.dag.dml.len(self, name=name, doc=doc))

    def type(self, *, name=None, doc=None) -> Node:
        return Node(self.dag, self.dag.dml.type(self, name=name, doc=doc))

    def items(self):
        for k in self:
            yield k, self[k]

    def value(self):
        return self.dag.dml.get_node_value(self.ref)
