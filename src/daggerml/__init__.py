from importlib.metadata import PackageNotFoundError, version

from daggerml.core import Dag, Dml, Error, Node, Ref, Resource, from_json, to_json

try:
    __version__ = version("daggerml")
except PackageNotFoundError:
    __version__ = 'local'

del version, PackageNotFoundError

__all__ = ('Dml', 'Dag', 'Error', 'Node', 'Ref', 'Resource', 'from_json', 'to_json')
