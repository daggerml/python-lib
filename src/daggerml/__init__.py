from importlib.metadata import PackageNotFoundError, version

import daggerml.util as _util
from daggerml.api import Dag, Node
from daggerml.api import load_state_file as load
from daggerml.core import Datum, Error, Ref, Resource, Scalar
from daggerml.util import ApiError

try:
    __version__ = version("daggerml")
except PackageNotFoundError:
    __version__ = 'local'

del version, PackageNotFoundError

def set_flags(kw):
    _util.CLI_FLAGS = _util.CLI_FLAGS.update(kw)

def revert_flags():
    _util.CLI_FLAGS = _util.CLI_FLAGS.revert()


__all__ = (
    'Dag', 'Datum', 'Error', 'Node', 'Ref', 'Resource', 'Scalar', 'ApiError'
)
