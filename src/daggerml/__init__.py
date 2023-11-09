from importlib.metadata import PackageNotFoundError, version

from daggerml.core import Dag, Datum, Error, Fn, Literal, Load, Node, Ref, Repo, Resource, Scalar, create_dag
from daggerml.util import ApiError

try:
    __version__ = version("daggerml")
except PackageNotFoundError:
    __version__ = 'local'

del version, PackageNotFoundError


__all__ = (
    'Dag', 'Datum', 'Error', 'Fn', 'Literal', 'Load', 'Node', 'Ref',
    'Repo', 'Resource', 'Scalar', 'ApiError', 'create_dag'
)
