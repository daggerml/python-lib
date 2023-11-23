import logging
from dataclasses import dataclass

from daggerml import core

logger = logging.getLogger(__name__)


@dataclass
class Node:
    ref: core.Ref
    dag: "Dag"

    @property
    def value(self):
        return self.ref().value().value

    def __call__(self, *args: "Node") -> "Dag":
        return self.dag.apply(self, *args)


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

    def commit(self, result) -> Node|None:
        result = self.repo.commit(result.ref)
        if result is None:
            return
        assert isinstance(result, core.Ref)
        return Node(result, self)

    def apply(self, resource: Node, *args: Node) -> "Dag":
        assert isinstance(resource.value, core.Resource)
        repo = self.repo.begin([resource.ref] + [x.ref for x in args])
        assert isinstance(repo, core.Repo)
        dag = Dag(repo=repo)
        return dag

    @property
    def expr(self):
        dag = self.repo.dag()
        if not hasattr(dag, 'expr'):
            raise ValueError('cannot access `expr` for a non function dag')
        return [Node(x, self) for x in dag.expr]

    def __enter__(self):
        return self

    def __exit__(self, err_type, exc_val, err_tb):
        if exc_val is not None:
            ex = core.Error.from_ex(exc_val)
            logger.exception('failing dag with error code: %r', ex.code)
            self.commit(ex)
