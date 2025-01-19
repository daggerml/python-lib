import json
import logging
import shutil
import subprocess
import traceback as tb
from dataclasses import dataclass, field, fields, replace
from typing import Any, List, NewType, overload

from daggerml.util import kwargs2opts, raise_ex

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
            self.context = {'trace': tb.format_exception(type(ex), value=ex, tb=ex.__traceback__)}
            self.code = type(ex).__name__
        else:
            self.code = type(self).__name__ if self.code is None else self.code

    def __str__(self):
        return ''.join(self.context.get('trace', [self.message]))


@dataclass
class Dml:  # noqa: F811
    opts: list = field(default_factory=list)
    token: str | None = None

    @classmethod
    def init(cls, **kwargs):
        return cls(kwargs2opts(kwargs))

    def __call__(self, *args: list[str], as_text: bool = False) -> Any:
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

    def new(self, name: str, message: str, dag_dump: str | None = None) -> Dag:
        opts = [] if not dag_dump else ['--dag-dump', dag_dump]
        token = self('dag', 'create', *opts, name, message, as_text=True)
        return Dag(self, token)


@dataclass
class Dag:  # noqa: F811
    dml: Dml
    token: str
    result: Node | None = None

    def __post_init__(self):
        self.dml = replace(self.dml, token=self.token)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value is not None:
            self.commit(Error(exc_value))

    @property
    def expr(self) -> Node:
        ref = self.dml.get_expr()
        assert isinstance(ref, Ref)
        return Node(self, ref)

    def put(self, value: Scalar | Collection) -> Node:
        assert not isinstance(value, Node) or value.dag == self
        return Node(self, self.dml.put_literal(value))

    def commit(self, value) -> Node:
        if isinstance(value, Error):
            pass
        val = value if isinstance(value, (Node, Error)) else self.put(value)
        self.result = Node(self, self.dml.commit(val))
        return self.result

    def dump(self, node):
        return self.dml('ref', 'dump', to_json(node.ref))


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
            # Get the start, stop, and step from the slice
            return [self[i] for i in range(*key.indices(len(self)))]
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

    def __call__(self, *args, **kwargs) -> Node:
        while True:
            resp = raise_ex(self.dag.dml.start_fn([self, *args], **kwargs))
            if resp:
                return Node(self.dag, resp)

    def keys(self) -> Node:
        return Node(self.dag, self.dag.dml.keys(self))

    def len(self) -> Node:
        return Node(self.dag, self.dag.dml.len(self))

    def type(self) -> Node:
        return Node(self.dag, self.dag.dml.type(self))

    def items(self):
        for k in self:
            yield k, self[k]

    def value(self):
        return self.dag.dml.get_node_value(self.ref)
