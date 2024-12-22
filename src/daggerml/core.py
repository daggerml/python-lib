import json
import logging
import subprocess
import traceback as tb
from dataclasses import InitVar, dataclass, field, fields
from tempfile import TemporaryDirectory
from typing import Callable, Dict, List, Optional, overload

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
    uri: str
    data: str = ''

    @property
    def js(self):
        return json.loads(self.data)

    @property
    def scheme(self):
        car, cdr = self.uri.split(':', 1)
        return car

    @property
    def id(self):
        car, cdr = self.uri.split(':', 1)
        return cdr

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
    cache_key: str
    dump: str
    dag: "Dag"
    resource_data: str

    def get_result(self):
        ref = self.dag._invoke('get_fn_result', self.ref)
        if ref is None:
            return
        assert isinstance(ref, Ref)
        return Node(self.dag, ref)


@dataclass
class FnUpdater(FnWaiter):
    update_fn: Callable[[str, str, str], str|None]

    @classmethod
    def from_waiter(cls, waiter, update_fn):
        f = {k.name: getattr(waiter, k.name) for k in fields(waiter)}
        out = cls(update_fn=update_fn, **f)
        out.update()
        return out

    def update(self):
        resp = self.get_result()
        if resp is not None:
            return resp
        logger.info('Updater is not finished yet... updating now.')
        try:
            resp = self.update_fn(self.cache_key, self.dump, self.resource_data)
            if resp is not None:
                logger.info('found non null resp... Loading %r into %r now.', resp, self.dag.tok)
                self.dag.load_ref(resp)
        except Exception as e:
            dag = self.dag.api.new_dag('asdf', 'qwer', dump=self.dump)
            dag.commit(Error.from_ex(e))
        return self.get_result()


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
        resp = subprocess.run(cmd, capture_output=True, check=True)
        return resp.stdout.decode().strip()
    except KeyboardInterrupt:
        raise
    except Exception as e:
        raise Error.from_ex(e) from e


@dataclass
class Api:
    config_dir: InitVar[str|None] = None
    project_dir: InitVar[str|None] = None
    initialize: InitVar[bool] = False
    tmpdirs: List[TemporaryDirectory] = field(default_factory=list)
    flags: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self, config_dir, project_dir, initialize):
        if initialize:
            if config_dir is None and 'config-dir' not in self.flags:
                tmpd = TemporaryDirectory()
                self.tmpdirs.append(tmpd)
                self.flags['config-dir'] = tmpd.__enter__()
            if project_dir is None and 'project-dir' not in self.flags:
                tmpd = TemporaryDirectory()
                self.tmpdirs.append(tmpd)
                self.flags['project-dir'] = tmpd.__enter__()
            self.init()
        if config_dir is not None:
            self.flags['config-dir'] = config_dir
        if project_dir is not None:
            self.flags['project-dir'] = project_dir

    def init(self):
        self('repo', 'create', 'test')
        self('project', 'init', 'test')

    @staticmethod
    def _to_flags(flag_dict: Dict[str, str], **kw: str) -> List[str]:
        out = []
        flag_dict = dict(**flag_dict, **kw)
        for k, v in sorted(flag_dict.items()):
            out.extend([f'--{k}', v])
        return out

    def __call__(self, *args, output='text'):
        resp = _api(*self._to_flags(self.flags, output=output), *args)
        if output == 'json':
            return [json.loads(x) for x in resp.split('\n') if len(x) > 0]
        return resp

    def __enter__(self):
        return self

    def __exit__(self, *x, **kw):
        for d in self.tmpdirs:
            d.__exit__(*x, **kw)

    def load(self, dump):
        resp = self('repo', 'load-ref', dump)
        return resp

    def dump(self, ref):
        resp = self('repo', 'dump-ref', to_json(ref))
        return resp

    def new_dag(self,
                name: str|None,
                message: str,
                dump: Optional[str] = None) -> "Dag":
        return Dag.new(name, message, dump=dump, api=self)

@dataclass(frozen=True)
class Node:
    dag: "Dag"
    ref: Ref

    def value(self):
        return self.dag._invoke('get_node_value', self.ref)

    def _db_ex(self, fn_name, *x):
        result = self.dag.start_fn(Resource(f'daggerml:op/{fn_name}'), self, *x)
        result = result.get_result()
        assert result is not None
        return result

    def keys(self) -> "Node":
        return self._db_ex('keys')

    @overload
    def __getitem__(self, key: slice) -> List["Node"]:
        ...
    @overload
    def __getitem__(self, key: str|int) -> "Node":
        ...
    @overload
    def __getitem__(self, key: "Node") -> "Node":
        ...

    def __getitem__(self, key):
        if isinstance(key, slice):
            # Get the start, stop, and step from the slice
            return [self[i] for i in range(*key.indices(len(self)))]
        return self._db_ex('get', key)

    def len(self) -> "Node":
        return self._db_ex('len')

    def type(self) -> "Node":
        return self._db_ex('type')

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

    def items(self):
        for k in self:
            yield k, self[k]

@dataclass
class Dag:
    tok: str
    api: Api
    result: Ref|None = None

    @classmethod
    def new(cls, name: str|None, message: str,
            dump: Optional[str] = None,
            api_flags: Dict[str, str]|None = None,
            api: Optional[Api] = None) -> "Dag":
        if api is None:
            api = Api(flags=api_flags or {})
        extra = [] if dump is None else ['--dag-dump', dump]
        tok = api('dag', 'create', *extra, name, message)
        assert isinstance(tok, str)
        return cls(tok, api)

    @property
    def expr(self) -> Node:
        ref = self._invoke('get_expr')
        assert isinstance(ref, Ref)
        return Node(self, ref)

    def _invoke(self, op, *args, **kwargs):
        payload = to_json([op, args, kwargs])
        resp = self.api('dag', 'invoke', self.tok, payload)
        data = from_json(resp)
        if isinstance(data, Error):
            raise data
        return data

    def put(self, data) -> Node:
        if isinstance(data, Node):
            if data.dag != self:
                raise ValueError('asdf')
            return data
        resp = self._invoke('put_literal', data)
        assert isinstance(resp, Ref)
        return Node(self, resp)

    def load(self, dag_name) -> Node:
        resp = self._invoke('put_load', dag_name)
        assert isinstance(resp, Ref)
        return Node(self, resp)

    def start_fn(self, *expr):
        expr = [x if isinstance(x, Node) else self.put(x) for x in expr]
        ref, cache_key, dump = self._invoke('start_fn', expr=[x.ref for x in expr])
        rsrc = expr[0].value()
        assert isinstance(rsrc, Resource)
        return FnWaiter(ref, cache_key, dump, self, rsrc.data)

    def commit(self, result) -> Ref:
        if not isinstance(result, (Node, Error)):
            result = self.put(result)
        if isinstance(result, Node):
            result = result.ref
        resp = self._invoke('commit', result)
        assert isinstance(resp, Ref)
        self.result = resp
        return resp

    def __enter__(self):
        return self

    def __exit__(self, err_type, exc_val, err_tb):
        if exc_val is not None:
            ex = Error.from_ex(exc_val)
            logger.exception('failing dag with error code: %r', ex.code)
            self.commit(ex)

    def load_ref(self, dump):
        resp = self.api.load(dump)
        return resp

    def dump(self, ref):
        resp = self.api.dump(ref)
        return resp
