from importlib.metadata import PackageNotFoundError, version

from daggerml._dag import (
    ApiError,
    Dag,
    DagError,
    DmlError,
    ListNode,
    MapNode,
    Node,
    NodeError,
    Resource,
    delete_dag,
    describe_dag,
    describe_node,
    get_dag_by_name_version,
    get_dag_topology,
    list_dags,
    register_tag,
)
from daggerml.contrib.s3 import S3Resource
from daggerml.contrib.util import cached_executor

try:
    __version__ = version("daggerml")
except PackageNotFoundError:
    __version__ = 'local'

del version, PackageNotFoundError
