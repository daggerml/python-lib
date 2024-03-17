#!/usr/bin/env python3
import json
import logging
import subprocess
import traceback as tb
from dataclasses import dataclass, field, fields
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

DATA_TYPE = {}


def dml_type(cls=None):
    def decorator(cls):
        DATA_TYPE[cls.__name__] = cls
        return cls
    return decorator(cls) if cls else decorator


@dml_type
@dataclass  # (frozen=True)
class Resource:
    data: Dict[str,str]

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


@dml_type
@dataclass(frozen=True)
class Ref:
    to: str

    @property
    def type(self):
        return self.to.split('/')[0]


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
    # return Ref(data)


def to_data(obj):
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
    return json.dumps(to_data(obj), separators=(',', ':'))


class ApiError(Error):
    pass


def _api(*args):
    try:
        cmd = ['dml', *args]
        logger.debug('cmd args: %r', cmd)
        resp = subprocess.run(cmd, capture_output=True)
        logger.debug('response: %r', resp.stdout)
        return resp.stdout.decode()
    except KeyboardInterrupt:
        raise
    except Exception as e:
        raise Error.from_ex(e) from e


def invoke_api(token, op, *args, **kwargs):
    payload = to_json([op, args, kwargs])
    resp = _api('dag', 'invoke', token, payload)
    data = from_json(resp)
    if isinstance(data, Error):
        raise data
    return data

@dataclass(frozen=True)
class Node:
    ref: Ref

@dataclass(frozen=True)
class Dag:
    token: str
    api_flags: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def new(cls, name: str, message: str, api_flags: Dict[str, str]|None = None) -> "Dag":
        api_flags = api_flags or {}
        tok = _api(*cls._to_flags(api_flags), 'dag', 'create', name, message)
        return cls(tok, api_flags=api_flags.copy())

    @staticmethod
    def _to_flags(flag_dict: Dict[str, str]) -> List[str]:
        out = []
        for k, v in sorted(flag_dict.items()):
            out.extend([f'--{k}', v])
        return out

    def _api(self, *args):
        return _api(*self._to_flags(self.api_flags), *args)

    def _invoke(self, op, *args, **kwargs):
        payload = to_json([op, args, kwargs])
        resp = self._api('dag', 'invoke', self.token, payload)
        return resp

    def invoke(self, op, *args, **kwargs):
        resp = self._invoke(op, *args, **kwargs)
        data = from_json(resp)
        if isinstance(data, Error):
            raise data
        return data

    def start_fn(self, *expr, cache: bool = True, retry: bool = False) -> "Dag|Node":
        expr = [x if isinstance(x, Node) else self.put(x) for x in expr]
        expr = [x.ref for x in expr]
        token = self._invoke('start_fn', expr=expr, cache=cache, retry=retry)
        ref = from_json(token)
        assert isinstance(ref, Ref)
        if ref.type == 'node':
            return Node(ref)
        return Dag(token, self.api_flags.copy())

    def call(self, f, *args, cache: bool = True, resource: Resource|None = None) -> "Dag|Node":
        resource = resource or Resource({'exec': 'local'})
        fndag = self.start_fn(self.put(resource), *args, cache=cache)
        if isinstance(fndag, Node):
            return fndag
        with fndag:
            result = f(*fndag.expr[1:])
            result = fndag.put(result)
            node = fndag.commit(result, cache=cache)
            assert isinstance(node, Node)
            return node

    @property
    def expr(self):
        return self.invoke('get_expr')

    def put(self, data) -> Node:
        resp = self.invoke('put_literal', data)
        assert isinstance(resp, Ref)
        return Node(resp)

    def load(self, dag_name) -> Node:
        resp = self.invoke('put_load', dag_name)
        assert isinstance(resp, Ref)
        return Node(resp)

    def commit(self, result, cache: bool = True) -> Node|None:
        if isinstance(result, Node):
            result = result.ref
        resp = self.invoke('commit', result, cache=cache)
        if resp is None:
            return
        assert isinstance(resp, Ref)
        return Node(resp)

    def get_value(self, node: Node) -> Any:
        val = self.invoke('get_node_value', node.ref)
        return val

    def __enter__(self):
        return self

    def __exit__(self, err_type, exc_val, err_tb):
        if exc_val is not None:
            ex = Error.from_ex(exc_val)
            logger.exception('failing dag with error code: %r', ex.code)
            self.commit(ex)
