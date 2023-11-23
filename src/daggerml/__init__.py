from importlib.metadata import PackageNotFoundError, version

from daggerml.api import Dag, Node
from daggerml.core import Datum, Error, Resource, Scalar
from daggerml.util import ApiError

try:
    __version__ = version("daggerml")
except PackageNotFoundError:
    __version__ = 'local'

del version, PackageNotFoundError


__all__ = (
    'Dag', 'Datum', 'Error', 'Node', 'Resource', 'Scalar', 'ApiError'
)
