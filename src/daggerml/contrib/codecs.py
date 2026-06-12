from __future__ import annotations

import importlib
from dataclasses import dataclass
from io import BytesIO
from typing import Any

from daggerml import Ref, Uri
from daggerml.api import Dag, DmlRepoError, apply_codecs
from daggerml.contrib.adapters import get_adapter
from daggerml.contrib.s3 import S3Store


class PandasDataFrameCodec:
    def __init__(self, dataframe_type: type[Any]):
        self._dataframe_type = dataframe_type

    def can_encode(self, value: Any) -> bool:
        return isinstance(value, self._dataframe_type)

    def encode(self, value: Any, dag: Dag) -> Uri:
        buf = BytesIO()
        value.to_parquet(buf)
        return S3Store().put(data=buf.getvalue(), suffix=".parquet")


class PolarsDataFrameCodec:
    def __init__(self, dataframe_type: type[Any]):
        self._dataframe_type = dataframe_type

    def can_encode(self, value: Any) -> bool:
        return isinstance(value, self._dataframe_type)

    def encode(self, value: Any, dag: Dag) -> Uri:
        buf = BytesIO()
        value.write_parquet(buf)
        return S3Store().put(data=buf.getvalue(), suffix=".parquet")


@dataclass(frozen=True)
class DelayedRef:
    name: str


@dataclass(frozen=True)
class DelayedLoad:
    dagname: str
    nodename: str | None = None


@dataclass(frozen=True)
class DelayedRunnable:
    uri: str
    adapter: str
    sub: Any | "DelayedRunnable" | None
    kwargs: dict[str, Any]


class DelayedActionCodec:
    def can_encode(self, value: Any) -> bool:
        return isinstance(value, (DelayedRef, DelayedLoad, DelayedRunnable))

    def _resolve_load_ref(self, value: DelayedLoad, dag: "Dag") -> Ref:
        index = dag.dml.runtime.describe(dag._require_index_ref())
        commit_ref = index["parents"][0]
        dag_ref = dag.dml.show(revision=commit_ref.to)["dags"].get(value.dagname)
        if dag_ref is None:
            raise DmlRepoError(f"DAG not found: {value.dagname}")
        resolved = dag.dml.dag.describe(dag_ref)
        if value.nodename is None:
            node_ref = resolved["result"]
        else:
            node_ref = resolved["names"].get(value.nodename)
        if node_ref is None:
            raise DmlRepoError(f"Node '{value.nodename}' not found in DAG '{value.dagname}'")
        return dag.dml.runtime.put_import(dag._require_index_ref(), dag_ref, node=node_ref, name=None)

    def encode(self, value: DelayedRef | DelayedLoad | DelayedRunnable, dag: "Dag"):
        if isinstance(value, DelayedRef):
            return apply_codecs(dag[value.name], dag=dag)
        if isinstance(value, DelayedLoad):
            # FIXME: should return with a node ref, not the python value
            return dag.dml.dag.get_node(self._resolve_load_ref(value, dag), recursive=True)
        adapter_spec = get_adapter(value.adapter)
        # no need for `apply_codecs` because we return a Runnable which is recursed
        return adapter_spec.resolve_runnable(value.uri, sub=value.sub, kwargs=value.kwargs)


def _import_optional(module_name: str) -> Any | None:
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as e:
        if e.name == module_name:
            return None
        raise


def literal_codecs() -> list[Any]:
    codecs: list[Any] = [(1, DelayedActionCodec())]
    pandas = _import_optional("pandas")
    if pandas is not None:
        codecs.append((1, PandasDataFrameCodec(pandas.DataFrame)))
    polars = _import_optional("polars")
    if polars is not None:
        codecs.append((1, PolarsDataFrameCodec(polars.DataFrame)))
    return codecs
