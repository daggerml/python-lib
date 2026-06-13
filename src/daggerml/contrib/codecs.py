from __future__ import annotations

import importlib
from dataclasses import dataclass
from tempfile import NamedTemporaryFile
from typing import Any

from daggerml import Uri
from daggerml.api import Dag, apply_codecs
from daggerml.contrib.adapters import get_adapter
from daggerml.contrib.s3 import S3Store


class PandasDataFrameCodec:
    def __init__(self, dataframe_type: type[Any]):
        self._dataframe_type = dataframe_type

    def can_encode(self, value: Any) -> bool:
        return isinstance(value, self._dataframe_type)

    def encode(self, value: Any, dag: Dag) -> Uri:
        with NamedTemporaryFile(suffix=".parquet") as tmp:
            value.to_parquet(tmp.name)
            return S3Store().put(filepath=tmp.name, suffix=".parquet")


class PolarsDataFrameCodec:
    def __init__(self, dataframe_type: type[Any]):
        self._dataframe_type = dataframe_type

    def can_encode(self, value: Any) -> bool:
        return isinstance(value, self._dataframe_type)

    def encode(self, value: Any, dag: Dag) -> Uri:
        with NamedTemporaryFile(suffix=".parquet") as tmp:
            value.write_parquet(tmp.name)
            return S3Store().put(filepath=tmp.name, suffix=".parquet")


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

    def encode(self, value: DelayedRef | DelayedLoad | DelayedRunnable, dag: "Dag"):
        if isinstance(value, DelayedRef):
            return apply_codecs(dag[value.name], dag=dag)  # apply_codecs required for `Node` objects.
        if isinstance(value, DelayedLoad):
            return apply_codecs(dag.require(value.dagname, name=value.nodename), dag=dag)  # apply_codecs required
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
