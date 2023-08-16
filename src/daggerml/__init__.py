from importlib.metadata import PackageNotFoundError, version

from daggerml._dag import (
    ApiError,
    Dag,
    DagError,
    DmlError,
    LocalResource,
    Node,
    NodeError,
    Resource,
    S3Resource,
    dag_fn,
    delete_dag,
    describe_dag,
    describe_node,
    get_dag_by_name_version,
    hash_object,
    list_dags,
    register_tag,
    s3_upload,
)

try:
    __version__ = version("daggerml")
except PackageNotFoundError:
    __version__ = 'local'

del version, PackageNotFoundError
