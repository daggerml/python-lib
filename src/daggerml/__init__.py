from importlib.metadata import PackageNotFoundError, version

from daggerml.core import Dag, Error, Node, Resource, _api, from_data, from_json, to_data, to_json

# import daggerml.util as _util
# from daggerml.api import Dag, Node, dump_obj, load_obj
# from daggerml.api import load_state_file as load
# from daggerml.core import Datum, Error, Ref, Resource, Scalar
# from daggerml.util import ApiError, from_data, from_json, to_data, to_json

try:
    __version__ = version("daggerml")
except PackageNotFoundError:
    __version__ = 'local'

del version, PackageNotFoundError

def new(name, message, flags=None):
    return Dag.new(name, message, api_flags=flags)

__all__ = (
    'Dag', 'Error', 'Node', 'Resource', 'from_data', 'to_data', 'from_json', 'to_json', 'new'
)
