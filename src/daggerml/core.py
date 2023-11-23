import logging

from daggerml.util import Error, dml_type, invoke_api

logger = logging.getLogger(__name__)


@dml_type
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

    def commit(self, result):
        return self._invoke('commit', result)

    def deref(self, ref):
        raise Exception('get_ref')

    def __enter__(self):
        return self

    def __exit__(self, err_type, exc_val, err_tb):
        if exc_val is not None:
            ex = Error.from_ex(exc_val)
            logger.exception('failing dag with error code: %r', ex.code)
            self.commit(ex)


def create_dag(name: str, message: str|None = None) -> Repo:
    repo = invoke_api(None, 'begin', name=name, message=message)
    assert isinstance(repo, Repo)  # make my fucking linter happy
    return repo


@dml_type
class Dag:
    nodes: set[Ref]  # -> node
    result: Ref | None  # -> node
    error: Error | None


@dml_type
class FnDag(Dag):
    expr: list[Ref]  # -> node


@dml_type
class CachedFnDag(Dag):
    expr: list[Ref]  # -> node


@dml_type
class Resource:
    data: dict


Scalar = str | int | float | bool | type(None) | Resource


@dml_type
class Datum:
    value: list | dict | set | Scalar


@dml_type
class Literal:
    value: Ref  # -> datum

    @property
    def error(self):
        pass


@dml_type
class Load:
    dag: Ref

    @property
    def value(self):
        return self.dag().result().value

    @property
    def error(self):
        return self.dag().result().error


@dml_type
class Fn(Load):
    expr: list[Ref]  # -> node


@dml_type
class Node:
    data: Literal | Load | Fn

    @property
    def value(self):
        return self.data.value

    @property
    def error(self):
        return self.data.error
