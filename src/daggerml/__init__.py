from importlib.metadata import PackageNotFoundError, version

from daggerml.core import Dag, Dml, Error, Node, Ref, Resource, from_json, to_json

try:
    __version__ = version("daggerml")
except PackageNotFoundError:
    __version__ = 'local'

del version, PackageNotFoundError


def new(name: str, message: str, dag_dump: str | None = None) -> Dag:
    return Dml().new(name, message, dag_dump=dag_dump)


__all__ = ('Dml', 'Dag', 'Error', 'Node', 'Ref', 'Resource', 'from_json', 'to_json')
