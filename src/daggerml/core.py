#!/usr/bin/env python3
import json
import logging
import subprocess
import traceback as tb
from dataclasses import dataclass, field, fields
from typing import Dict, List

logger = logging.getLogger(__name__)

DATA_TYPE = {}


def dml_type(cls=None):
    def decorator(cls):
        DATA_TYPE[cls.__name__] = cls
        return cls
    return decorator(cls) if cls else decorator


def js_dumps(js):
    return json.dumps(js, separators=(',', ':'))


@dml_type
@dataclass(frozen=True, slots=True)
class Resource:
    data: str

    @property
    def info(self):
        return json.loads(self.data)

    @classmethod
    def from_dict(cls, d):
        return cls(js_dumps(d))

Scalar = str | int | float | bool | type(None) | Resource

@dml_type
@dataclass
class Error(Exception):
    message: str
    context: dict = field(default_factory=dict)
    code: str|None = None

    def __post_init__(self):
        self.code = type(self).__name__ if self.code is None else self.code

    @classmethod
    def from_ex(cls, ex):
        if isinstance(ex, Error):
            return ex
        formatted_tb = tb.format_exception(type(ex), value=ex, tb=ex.__traceback__)
        return cls(str(ex), {'trace': formatted_tb}, type(ex).__name__)

    def __str__(self):
        msg = str(super())
        msg += f'\n\ndml.Error({self.code}: {self.message})'
        if 'trace' in self.context:
            sep = '='*80
            msg += f'\n{sep}\nTrace\n{sep}\n' + '\n'.join(self.context['trace'])
        return msg


@dml_type
@dataclass(frozen=True)
class Ref:
    to: str

    @property
    def type(self):
        return self.to.split('/')[0]


@dml_type
@dataclass
class FnWaiter:
    ref: Ref
    dump: str
    dag: "Dag"

    def get_result(self):
        ref = self.dag._invoke('get_fn_result', self.ref)
        if ref is None:
            return
        assert isinstance(ref, Ref)
        return Node(self.dag, ref)

    def cache(self):
        self.dag._invoke('populate_cache', self.ref)


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
    raise ValueError(f'cannot `from_data` {data!r}')


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
    raise ValueError(f'no data encoding for type: {n}')


def from_json(text):
    return from_data(json.loads(text))


def to_json(obj):
    return js_dumps(to_data(obj))


def _api(*args):
    try:
        cmd = ['dml', *args]
        resp = subprocess.run(cmd, capture_output=True)
        return resp.stdout.decode()
    except KeyboardInterrupt:
        raise
    except Exception as e:
        raise Error.from_ex(e) from e

@dataclass(frozen=True)
class Node:
    dag: "Dag"
    ref: Ref

    def value(self):
        return self.dag._invoke('get_node_value', self.ref)

@dataclass(frozen=True)
class Dag:
    tok: str
    api_flags: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def new(cls, name: str, message: str, dump: str|None = None, api_flags: Dict[str, str]|None = None) -> "Dag":
        api_flags = api_flags or {}
        extra = [] if dump is None else ['--dag-dump', dump]
        tok = _api(*cls._to_flags(api_flags), 'dag', 'create', name, message, *extra)
        return cls(tok, api_flags=api_flags.copy())

    @property
    def expr(self) -> List[Node]:
        return self._invoke('get_expr')

    @staticmethod
    def _to_flags(flag_dict: Dict[str, str]) -> List[str]:
        out = []
        for k, v in sorted(flag_dict.items()):
            out.extend([f'--{k}', v])
        return out

    def _invoke(self, op, *args, **kwargs):
        payload = to_json([op, args, kwargs])
        resp = _api(*self._to_flags(self.api_flags), 'dag', 'invoke', self.tok, payload)
        data = from_json(resp)
        if isinstance(data, Error):
            raise data
        return data

    def put(self, data) -> Node:
        resp = self._invoke('put_literal', data)
        assert isinstance(resp, Ref)
        return Node(self, resp)

    def load(self, dag_name) -> Node:
        resp = self._invoke('put_load', dag_name)
        assert isinstance(resp, Ref)
        return Node(self, resp)

    def start_fn(self, *expr, use_cache: bool = True):
        expr = [x if isinstance(x, Node) else self.put(x) for x in expr]
        expr = [x.ref for x in expr]
        ref, dump = self._invoke('start_fn', expr=expr, use_cache=use_cache)
        return FnWaiter(ref, dump, self)

    def commit(self, result, cache: bool|None = None) -> Ref:
        if not isinstance(result, (Node, Error)):
            result = self.put(result)
        if isinstance(result, Node):
            result = result.ref
        if cache is None:
            cache = getattr(self, 'cache', None)
        resp = self._invoke('commit', result)
        assert isinstance(resp, Ref)
        return resp

    def __enter__(self):
        return self

    def __exit__(self, err_type, exc_val, err_tb):
        if exc_val is not None:
            ex = Error.from_ex(exc_val)
            logger.exception('failing dag with error code: %r', ex.code)
            self.commit(ex)
