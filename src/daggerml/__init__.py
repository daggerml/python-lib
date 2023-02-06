from daggerml._dag import (  # noqa: F401
    DmlError, DagError, ApiError, NodeError, Resource,
    list_dags, describe_dag, get_dag_by_name_version,
    Dag, Node, register_tag
)
from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution("daggerml").version
except DistributionNotFound:
    __version__ = 'local'

del get_distribution, DistributionNotFound
