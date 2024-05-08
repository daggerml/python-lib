from importlib.metadata import PackageNotFoundError, version

from daggerml.core import Dag, Error, FnDag, Node, Resource, _api, from_data, from_json, to_data, to_json

try:
    __version__ = version("daggerml")
except PackageNotFoundError:
    __version__ = 'local'

del version, PackageNotFoundError

def new(name, message, flags=None):
    return Dag.new(name, message, api_flags=flags)

__all__ = (
    'Dag', 'Error', 'FnDag', 'Node', 'Resource', 'from_data', 'to_data', 'from_json', 'to_json', 'new'
)
