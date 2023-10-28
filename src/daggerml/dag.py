import logging
import traceback as tb
from dataclasses import InitVar

from daggerml.util import Error, _api, dml_type, invoke_api

logger = logging.getLogger(__name__)


@dml_type
class Ref:
    to: str

    @property
    def type(self):
        return self.to.split('/', 1)[0] if self.to else None

    @property
    def name(self):
        return self.to.split('/', 1)[1] if self.to else None

    def __call__(self):
        # FIXME: maybe we have a "deref" endpoint instead?
        raise _api(self.type, 'get', self.to)


@dml_type
class Dag:
    nodes: set[Ref]  # -> node
    result: Ref  # -> node
    error: Error | None = None
    _token: InitVar[str | None] = None

    @classmethod
    def create_dag(cls, name, message):
        dag = cls(set(), None, None)
        _, dag._token = invoke_api(None, 'begin', name, message)
        return dag

    def _invoke(self, op, *args, **kwargs):
        resp, self._token = invoke_api(self._token, op, *args, **kwargs)
        if isinstance(resp, Node):
            self.nodes.add(resp)
        return resp

    def put_literal(self, data):
        return self._invoke('put_literal', data)

    def put_load(self, dag_name):
        return self._invoke('put_load', dag_name)

    def put_fn(self, expr, info=None, value=None, replacing=None):
        return self._invoke('put_fn', expr, info=info, value=value, replacing=replacing)

    def commit(self, result):
        return self._invoke('commit', result)

    def __enter__(self):
        return self

    def __exit__(self, _, exc_val, __):
        if exc_val is not None:
            if not isinstance(exc_val, Error):
                trace = {
                    'trace': tb.format_exception(type(exc_val), value=exc_val, tb=exc_val.__traceback__),
                }
                exc_val = Error(message=str(exc_val),
                                context=trace)
            logger.exception('failing dag with error code: %r', exc_val.code)
            self.commit(exc_val)


@dml_type
class Resource:
    data: dict


Scalar = str | int | float | bool | type(None) | Resource


@dml_type
class Datum:
    value: list | dict | set | Scalar


@dml_type
class Literal:
    value: Datum


@dml_type
class Load:
    dag: Dag
    value: Datum


@dml_type
class Fn:
    expr: list["Node"]
    value: Datum


@dml_type
class Node:
    node: Literal | Load | Fn

    @property
    def value(self):
        return self.node.value

    @property
    def error(self):
        return self.node.error
