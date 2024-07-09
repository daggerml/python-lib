from importlib.metadata import PackageNotFoundError, version

from daggerml.core import Dag, Error, Node, Ref, Resource, from_data, from_json, to_data, to_json

try:
    __version__ = version("daggerml")
except PackageNotFoundError:
    __version__ = 'local'

del version, PackageNotFoundError

new = Dag.new

__all__ = (
    'Dag', 'Error', 'Node', 'Ref', 'Resource', 'from_data', 'to_data', 'from_json', 'to_json', 'new'
)
