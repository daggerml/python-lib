from importlib.metadata import PackageNotFoundError, version

from daggerml.dag import Dag, Datum, Error, Fn, Literal, Load, Node, Ref, Resource, Scalar
from daggerml.util import ApiError

try:
    __version__ = version("daggerml")
except PackageNotFoundError:
    __version__ = 'local'

del version, PackageNotFoundError
