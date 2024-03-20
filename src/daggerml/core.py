#!/usr/bin/env python3
import json
import logging
import os
import subprocess
import traceback as tb
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field, fields
from threading import Event
from time import time
from typing import Any, Dict, List
from uuid import uuid4

logger = logging.getLogger(__name__)
uuid = uuid4().hex

DATA_TYPE = {}


def dml_type(cls=None):
    def decorator(cls):
        DATA_TYPE[cls.__name__] = cls
        return cls
    return decorator(cls) if cls else decorator


@dml_type
@dataclass(frozen=True, slots=True)
class Resource:
    data: str

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
        resp = subprocess.run(cmd, capture_output=True)
        return resp.stdout.decode()
    except KeyboardInterrupt:
        raise
    except Exception as e:
        raise Error.from_ex(e) from e

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

    def start_fn(self, *expr, cache: bool = True, retry: bool = False) -> "FnDag|Node":
        expr = [x if isinstance(x, Node) else self.put(x) for x in expr]
        expr = [x.ref for x in expr]
        ref = self.invoke('start_fn', expr=expr, cache=cache, retry=retry)
        assert isinstance(ref, Ref)
        if ref.type == 'node':
            return Node(ref)
        assert ref.type == 'index'
        token = to_json(ref)
        return FnDag(token, api_flags=self.api_flags.copy())

    def call(self, f, *args, cache: bool = True, retry: bool = False, update_freq: int|float = 5) -> Node:
        resource = Resource(json.dumps({'exec': 'local', 'uuid': uuid, 'func_name': f.__qualname__}, separators=(',', ':')))
        fndag = self.start_fn(self.put(resource), *args, cache=cache, retry=retry)
        if isinstance(fndag, Node):
            logger.debug('returning cached call')
            return fndag
        logger.debug('running call')
        lock_info = {'pid': os.getpid(), 'uid': uuid4().hex}
        try:
            lock = fndag.relock(lock_info, update_freq)
        except Error as e:
            raise RuntimeError(f'error: {e = } -- wtf')
        if lock is None:
            raise RuntimeError('lock was none?!')
        with fndag._update_loop(lock, lock_info, freq=update_freq):
            with fndag:
                result = f(*fndag.expr[1:])
                result = fndag.put(result)
                node = fndag.commit(result, cache=cache)
        assert isinstance(node, Node)
        return node

    def put(self, data) -> Node:
        resp = self.invoke('put_literal', data)
        assert isinstance(resp, Ref)
        return Node(resp)

    def load(self, dag_name) -> Node:
        resp = self.invoke('put_load', dag_name)
        assert isinstance(resp, Ref)
        return Node(resp)

    def commit(self, result, cache: bool = True) -> None:
        if isinstance(result, Node):
            result = result.ref
        resp = self.invoke('commit', result, cache=cache)
        assert resp is None
        return resp

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

@dataclass(frozen=True)
class FnDag(Dag):
    expr: List[Node] = field(init=False)

    def __post_init__(self):
        resp = self.invoke('get_expr')
        object.__setattr__(self, 'expr', resp)

    @property
    def meta(self) -> str:
        return self.invoke('get_fn_meta')

    def update_meta(self, old: str, new: str) -> bool:
        return self.invoke('update_fn_meta', old, new)

    def lockable(self, meta: str|None = None, old: str|None = None) -> bool:
        if meta is None:
            meta = self.meta
        if old is not None and meta != old:
            return False
        if meta == '':
            return True
        meta_js = from_json(meta)
        assert isinstance(meta_js, dict)
        return time() >= meta_js['lock_expiration']

    def relock(self, new: Dict, duration: float|int = 5, old: str|None = None) -> str|None:
        meta = self.meta
        if not self.lockable(meta, old):
            raise RuntimeError(f'not lockable {meta = }')
            logger.info('another valid lock exists')
            return
        new['lock_expiration'] = time() + duration
        new_str = to_json(new)
        try:
            self.update_meta(meta, new_str)
        except Error:
            raise RuntimeError(f'failed to update {meta = } ---- {new_str = }')
            logger.exception('failed to update metadata')
            return
        return new_str

    @contextmanager
    def _update_loop(self, lock, state_dict: Dict[str, Any], freq: int|float):
        lock_duration = 2 * (freq + 0.2)
        event = Event()
        def inner(old, freq):
            while event.wait(freq):
                if event.is_set():
                    return
                old = self.relock(state_dict, lock_duration, old)
        with ThreadPoolExecutor(max_workers=1) as executor:
            fut = executor.submit(inner, lock, freq)
            try:
                yield
            finally:
                event.set()
                fut.result()

    def commit(self, result, cache: bool = True) -> Node:
        if isinstance(result, Node):
            result = result.ref
        resp = self.invoke('commit', result, cache=cache)
        assert isinstance(resp, Ref)
        return Node(resp)
