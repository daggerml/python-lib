import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Set

from daggerml import core

logger = logging.getLogger(__name__)


@dataclass
class Node:
    ref: core.Ref
    dag: "Dag"

    @property
    def value(self) -> core.Scalar|List[core.Ref]|Set[core.Ref]|Dict[str, core.Ref]:
        res = self.ref()
        assert isinstance(res, core.Node)
        return res.value().value

    @property
    def error(self) -> core.Error|None:
        res = self.ref()
        assert isinstance(res, core.Node)
        return res.error

    def unroll(self) -> core.Scalar|List[Any]|Set[Any]|Dict[str, Any]:
        res = self.ref()
        assert isinstance(res, core.Node)
        if res.value is None:
            raise res.error
        return res.value().unroll()


@dataclass
class Dag:
    name: str|None = None
    message: str|None = None
    repo: core.Repo|None = None

    def __post_init__(self):
        if self.repo is None:
            assert self.name is not None
            self.repo = core.create_dag(self.name, message=self.message)
        elif (self.name and self.message) is not None:
            raise ValueError('name and message must be none when repo is specified')

    def put(self, data) -> Node:
        ref = self.repo.put_literal(data)
        return Node(ref, self)

    def load(self, dag_name) -> Node:
        ref = self.repo.put_load(dag_name)
        return Node(ref, self)

    def start_fn(self, resource: Node, *args: Node) -> "Dag":
        assert isinstance(resource.value, core.Resource)
        repo = self.repo.begin([x.ref for x in [resource, *args]])
        assert isinstance(repo, core.Repo)
        dag = Dag(repo=repo)
        return dag

    def commit(self, result: core.Error|Node|None, cache=None) -> Node|None:
        if cache is False and result is None:
            raise ValueError('cannot commit None result')
        if result is None:
            result_ref = None
        elif isinstance(result, core.Error):
            result_ref = result
        elif isinstance(result, Node):
            result_ref = result.ref
        else:
            raise ValueError('cannot commit type: %r' % type(result))
        if self.repo.parent_dag is not None:
            # function
            if cache is True:
                cache = self.repo.cached_dag
            elif cache is None and self.repo.cached_dag is None:
                cache = True
        res = self.repo.commit(result_ref, cache=cache)
        if res is None:
            # no parent dag
            return
        assert isinstance(res, core.Ref)
        # return Node(res, self.repo.parent_dag())
        return Node(res, self)

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
        if not isinstance(dag, core.FnDag):
            raise ValueError('cannot access `expr` for a non function dag')
        return [Node(x, self) for x in dag.expr]

    def __enter__(self):
        return self

    def __exit__(self, err_type, exc_val, err_tb):
        if exc_val is not None:
            ex = core.Error.from_ex(exc_val)
            logger.exception('failing dag with error code: %r', ex.code)
            self.commit(ex)
