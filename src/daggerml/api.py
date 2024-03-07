import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Set

from daggerml import core
from daggerml.core import Error, FnDag, Ref, Repo, Resource, Scalar, create_dag
from daggerml.util import OpaqueDbObj, from_data, invoke_api, to_data

logger = logging.getLogger(__name__)


@dataclass
class Node:
    ref: Ref
    dag: "Dag"

    @property
    def value(self) -> Scalar|List[Ref]|Set[Ref]|Dict[str, Ref]:
        res = self.ref()
        assert isinstance(res, core.Node)
        return res.value().value

    @property
    def error(self) -> Error|None:
        res = self.ref()
        assert isinstance(res, core.Node), f'aahh type: {type(res)}'
        return res.error

    def unroll(self) -> Scalar|List[Any]|Set[Any]|Dict[str, Any]:
        res = self.ref()
        if res is None:
            raise RuntimeError('are you in the wrong db?')
        assert isinstance(res, core.Node), f'aahh type: {type(res)}'
        if res.value is None:
            raise res.error
        return res.value().unroll()


def begin(repo: OpaqueDbObj, expr: dict|None = None) -> "Dag":
    new_repo = invoke_api(repo, 'begin', expr=expr)
    assert isinstance(new_repo, Repo)
    return new_repo

def put_literal(repo: OpaqueDbObj, data) -> Ref:
    return invoke_api(repo, 'put_literal', data)

def put_load(repo: OpaqueDbObj, dag_name) -> Ref:
    return invoke_api(repo, 'put_load', dag_name)

def commit(repo: OpaqueDbObj, result, cache=None) -> Ref|None:
    return invoke_api(repo, 'commit', result, cache)

def dump_obj(repo: OpaqueDbObj, obj: Any):
    return invoke_api(repo, 'dump', obj)

def load_obj(repo: OpaqueDbObj, obj: Any):
    return invoke_api(repo, 'load', obj)


@dataclass
class Dag:
    name: str|None = None
    message: str|None = None
    repo: Repo|None = None
    parent_dag: "Dag" = None

    def __post_init__(self):
        if self.repo is None:
            assert self.name is not None
            self.repo = create_dag(self.name, message=self.message)

    def dump_state(self) -> Any:
        def inner_fn(dag):
            if dag is None:
                return
            return dict(name=self.name,
                        message=self.message,
                        repo=self.repo,
                        parent_dag=inner_fn(dag.parent_dag))
        return to_data(inner_fn(self))

    @classmethod
    def from_state(cls, state: Any) -> "Dag":
        state = from_data(state)
        def inner_fn(_state):
            if _state is None:
                return
            _state['parent_dag'] = inner_fn(_state.get('parent_dag'))
            return cls(**_state)
        return inner_fn(state)

    def put(self, data) -> Node:
        # ref = self.repo.put_literal(data)
        ref = put_literal(self.repo, data)
        return Node(ref, self)

    def load(self, dag_name) -> Node:
        # ref = self.repo.put_load(dag_name)
        ref = put_load(self.repo, dag_name)
        return Node(ref, self)

    def start_fn(self, resource: Node, *args: Node) -> "Dag":
        assert isinstance(resource.value, Resource)
        # repo = self.repo.begin([x.ref for x in [resource, *args]])
        repo = begin(self.repo, [x.ref for x in [resource, *args]])
        dag = Dag(repo=repo, parent_dag=self)
        return dag

    def commit(self, result: Error|Node|None, cache=None) -> Node|None:
        if cache is False and result is None:
            raise ValueError('cannot commit None result')
        if result is None:
            result_ref = None
        elif isinstance(result, Error):
            result_ref = result
        elif isinstance(result, Node):
            result_ref = result.ref
        else:
            raise ValueError('cannot commit type: %r' % type(result))
        # function
        if cache is True:
            cache = self.repo.cached_dag
        elif cache is None and self.repo.cached_dag is None:
            cache = True
        res = commit(self.repo, result_ref, cache=cache)
        if res is None:
            # no parent dag
            return
        assert isinstance(res, Ref)
        # return Node(res, self.repo.parent_dag())
        return Node(res, self.parent_dag)

    def apply(self, f: Callable[["Dag"], Node],
              resource: Node,
              *args: Node,
              cache: bool|None = None) -> Node:
        """
        Parameters
        ----------
        f : Callable
        cache : bool|None
            None means use cache if available (including errors) and populate cache if needed
            True means force this execution (replace the cache if it exists. E.g. retry errors
            False means no caching (don't use the cache and don't cache the results)
        """
        with self.start_fn(resource, *args) as fndag:
            if (cache is None) and (fndag.repo.cached_dag is not None):
                result = fndag.commit(None)
            else:
                result = f(fndag)
                result = fndag.commit(result, cache=cache)
        assert result is not None
        return result

    @property
    def expr(self) -> List[Node]:
        dag = self.repo.dag()
        if not isinstance(dag, FnDag):
            raise ValueError('cannot access `expr` for a non function dag')
        return [Node(x, self) for x in dag.expr]

    def __enter__(self):
        return self

    def __exit__(self, err_type, exc_val, err_tb):
        if exc_val is not None:
            ex = Error.from_ex(exc_val)
            logger.exception('failing dag with error code: %r', ex.code)
            self.commit(ex)

def load_state_file(file_path: str) -> Dag:
    with open(file_path, 'r') as f:
        return Dag.from_state(json.load(f))
