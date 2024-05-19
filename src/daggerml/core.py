#!/usr/bin/env python3
import json
import logging
import os
import subprocess
import traceback as tb
from contextlib import contextmanager
from dataclasses import dataclass, field, fields
from pathlib import Path
from threading import Event, Thread
from time import sleep, time
from typing import Any, Dict, List
from uuid import uuid4

logger = logging.getLogger(__name__)
UUID = uuid4().hex

DATA_TYPE = {}

CACHE_LOC = Path.home()/'.local/dml/cache'


def dml_type(cls=None):
    def decorator(cls):
        DATA_TYPE[cls.__name__] = cls
        return cls
    return decorator(cls) if cls else decorator


def js_dumps(js):
    return json.dumps(js, separators=(',', ':'))


class NamespacedObj:
    @classmethod
    def register_ns(cls, ns_cls):
        setattr(cls, ns_cls.name, property(ns_cls))


@dml_type
@dataclass(frozen=True, slots=True)
class Resource(NamespacedObj):
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


@dml_type
@dataclass(frozen=True)
class Ref(NamespacedObj):
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
class Node(NamespacedObj):
    ref: Ref

@dataclass(frozen=True)
class Dag(NamespacedObj):
    tok: str
    ref: Ref
    api_flags: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def new(cls, name: str, message: str, api_flags: Dict[str, str]|None = None) -> "Dag":
        api_flags = api_flags or {}
        tok = _api(*cls._to_flags(api_flags), 'dag', 'create', name, message)
        tok, ref = from_json(tok)
        return cls(to_json(tok), ref, api_flags=api_flags.copy())

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
        resp = self._api('dag', 'invoke', self.tok, payload)
        return resp

    def invoke(self, op, *args, **kwargs):
        resp = self._invoke(op, *args, **kwargs)
        data = from_json(resp)
        if isinstance(data, Error):
            raise data
        return data

    def put(self, data) -> Node:
        resp = self.invoke('put_literal', self.ref, data)
        assert isinstance(resp, Ref)
        return Node(resp)

    def load(self, dag_name) -> Node:
        resp = self.invoke('put_load', self.ref, dag_name)
        assert isinstance(resp, Ref)
        return Node(resp)

    def start_fn(self, *expr, cache: bool = True, retry: bool = False) -> "FnDag|Node":
        expr = [x if isinstance(x, Node) else self.put(x) for x in expr]
        expr = [x.ref for x in expr]
        ref = self.invoke('start_fn', expr=expr, dag=self.ref, cache=cache, retry=retry)
        assert isinstance(ref, Ref)
        if ref.type == 'node':
            return Node(ref)
        return FnDag(tok=self.tok, ref=ref, par=self.ref, cache=cache, api_flags=self.api_flags.copy())

    def call(self, f, *args, cache: bool = True, retry: bool = False, update_freq: float|int = 5) -> Node:
        resource = Resource.from_dict({'exec': 'local', 'func_name': f.__qualname__})
        expr = [self.put(resource), *args]
        fndag = self.start_fn(*expr, cache=cache, retry=retry)
        if isinstance(fndag, Node):
            logger.debug('returning cached call')
            return fndag
        logger.debug('checking dag: %r', fndag.ref.to)
        lock_info = {'pid': os.getpid(), 'uid': uuid4().hex}
        lock = fndag.relock(lock_info, 5 * update_freq, old='')
        if lock is None:
            logger.debug('no lock. waiting for result...')
            while not fndag.lockable(fndag.meta, old=''):
                sleep(update_freq)
            resp = self.start_fn(*expr, cache=cache)
            if isinstance(resp, FnDag):
                err = Error('orphaned job')
                resp.commit(err)
                raise err
            return resp
        logger.debug('running function: %r', f.__qualname__)
        with fndag._update_loop(lock, lock_info, freq=update_freq):
            with fndag:
                result = f(*fndag.expr[1:])
            result = fndag.put(result)
            node = fndag.commit(result)
        assert isinstance(node, Node)
        return node

    def _commit(self, result, cache: bool|None = None) -> None|Ref:
        if not isinstance(result, (Node, Error)):
            result = self.put(result)
        if isinstance(result, Node):
            result = result.ref
        if cache is None:
            cache = getattr(self, 'cache', None)
        par = getattr(self, 'par', None)
        resp = self.invoke('commit', self.ref, result, parent_dag=par, cache=cache)
        return resp

    def commit(self, result) -> None:
        resp = self._commit(result)
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
    par: Ref = field(kw_only=True)
    expr: List[Node] = field(init=False)
    cache: bool = False

    def __post_init__(self):
        resp = self.invoke('get_expr', self.ref)
        object.__setattr__(self, 'expr', resp)

    @property
    def meta(self) -> str:
        return self.invoke('get_fn_meta', self.ref)

    def update_meta(self, old: str, new: str) -> bool:
        return self.invoke('update_fn_meta', self.ref, old, new)

    @staticmethod
    def lockable(meta: str, old: str|None = None) -> bool:
        if old is not None and meta != old:
            return False
        if meta == '':
            return True
        meta_js = from_json(meta)
        assert isinstance(meta_js, dict)
        # expiration has passed
        return meta_js['lock_expiration'] <= time()

    def relock(self, new: Dict, duration: float|int = 5, old: str|None = None) -> str|None:
        meta = self.meta
        if not self.lockable(meta, old):
            logger.info('another valid lock exists')
            return
        new['lock_expiration'] = time() + duration
        new_str = to_json(new)
        try:
            self.update_meta(meta, new_str)
        except Error:
            logger.exception('failed to update metadata')
            return
        return new_str

    @contextmanager
    def _update_loop(self, lock, state_dict: Dict[str, Any], freq: float|int):
        lock_duration = 2 * (freq + 0.2)
        event = Event()
        def inner(old, freq):
            while event.wait(freq):
                if event.is_set():
                    return
                old = self.relock(state_dict, lock_duration, old)
        thread = Thread(target=inner, args=(lock, freq))
        try:
            thread.start()
            yield event
        finally:
            event.set()
            thread.join()

    def commit(self, result, cache: bool|None = None) -> Node:
        resp = self._commit(result, cache=cache)
        assert isinstance(resp, Ref)
        assert resp.type == 'node'
        return Node(resp)
