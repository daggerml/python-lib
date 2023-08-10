from importlib.metadata import version, PackageNotFoundError

from daggerml._dag import (  # noqa: F401
    DmlError, DagError, ApiError, NodeError, Resource,
    list_dags, describe_dag, delete_dag, get_dag_by_name_version,
    Dag, Node, register_tag, dag_fn
)

try:
    __version__ = version("daggerml")
except PackageNotFoundError:
    __version__ = 'local'

del version, PackageNotFoundError
