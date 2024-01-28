import logging
from dataclasses import dataclass

from daggerml.util import Error, dml_type, invoke_api

logger = logging.getLogger(__name__)


@dml_type
@dataclass
class Ref:
    to: str | None

    @property
    def type(self):
        return self.to.split('/', 1)[0] if self.to else None

    @property
    def name(self):
        return self.to.split('/', 1)[1] if self.to else None

    def __call__(self):
        # FIXME: maybe we have a "deref" endpoint instead?
        return invoke_api(None, 'get_ref', self)

    def __hash__(self):
        return hash(self.to)


@dml_type
@dataclass
class Repo:
    path: str
    user: str
    head: Ref
    index: Ref | None
    dag: Ref | None
    parent_dag: Ref | None
    cached_dag: Ref | None
    create: bool

    def _invoke(self, op, *args, **kwargs):
        resp = invoke_api(self, op, *args, **kwargs)
        # if isinstance(resp, Node):
        #     self.nodes.add(resp)
        return resp

    def begin(self, expr) -> "Repo":
        repo = self._invoke('begin', expr=expr)
        assert isinstance(repo, Repo)
        return repo

    def put_literal(self, data) -> Ref:
        return self._invoke('put_literal', data)

    def put_load(self, dag_name) -> Ref:
        return self._invoke('put_load', dag_name)

    def commit(self, result, cache=None) -> Ref|None:
        return self._invoke('commit', result, cache)

    def deref(self, ref):
        raise Exception('get_ref')


def create_dag(name: str, message: str|None = None) -> Repo:
    repo = invoke_api(None, 'begin', name=name, message=message)
    assert isinstance(repo, Repo)  # make my fucking linter happy
    return repo


@dml_type
@dataclass
class Dag:
    nodes: set[Ref]  # -> node
    result: Ref | None  # -> node
    error: Error | None


@dml_type
@dataclass
class FnDag(Dag):
    expr: list[Ref]  # -> node


@dml_type
@dataclass
class CachedFnDag(Dag):
    expr: list[Ref]  # -> node


@dml_type
@dataclass
class Resource:
    """
    Represents an externally managed Datum eg. s3, kubernetes cluster
    """
    data: dict


Scalar = str | int | float | bool | type(None) | Resource


@dml_type
@dataclass
class Datum:
    value: list | dict | set | Scalar

    def unroll(self):
        val = self.value
        if isinstance(val, Scalar):
            return val
        if isinstance(val, list|tuple):
            return [x().unroll() for x in val]
        if isinstance(val, dict):
            return {k: v().unroll() for k, v in val.items()}
        if isinstance(val, set):
            return {v().unroll() for v in val}
        raise ValueError('unrecognized type: %r' % type(val))


@dml_type
@dataclass
class Literal:
    value: Ref  # -> datum

    @property
    def error(self):
        pass


@dml_type
@dataclass
class Load:
    dag: Ref

    @property
    def value(self):
        dag = self.dag()
        assert isinstance(dag, Dag)
        if dag.result is None:
            if dag.error is None:
                raise RuntimeError('cannot get dag value for unfinished dag')
            raise dag.error
        return dag.result().value

    @property
    def error(self) -> Error:
        return self.dag().error


@dml_type
@dataclass
class Fn(Load):
    expr: list[Ref]  # -> node


@dml_type
@dataclass
class Node:
    data: Literal | Load | Fn

    @property
    def value(self):
        return self.data.value

    @property
    def error(self) -> Error:
        return self.data.error
