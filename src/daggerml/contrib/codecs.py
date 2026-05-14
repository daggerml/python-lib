from __future__ import annotations

import importlib
from io import BytesIO
from typing import Any

from daggerml import Uri
from daggerml.contrib.s3 import S3Store


def _import_optional(module_name: str) -> Any | None:
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as e:
        if e.name == module_name:
            return None
        raise


class PandasDataFrameCodec:
    def __init__(self, dataframe_type: type[Any]):
        self._dataframe_type = dataframe_type

    def can_encode(self, value: Any) -> bool:
        return isinstance(value, self._dataframe_type)

    def encode(self, value: Any, ctx: Any) -> Uri:
        buf = BytesIO()
        value.to_parquet(buf)
        return S3Store().put(data=buf.getvalue(), suffix=".parquet")


class PolarsDataFrameCodec:
    def __init__(self, dataframe_type: type[Any]):
        self._dataframe_type = dataframe_type

    def can_encode(self, value: Any) -> bool:
        return isinstance(value, self._dataframe_type)

    def encode(self, value: Any, ctx: Any) -> Uri:
        buf = BytesIO()
        value.write_parquet(buf)
        return S3Store().put(data=buf.getvalue(), suffix=".parquet")


def literal_codecs() -> list[Any]:
    codecs: list[Any] = []

    pandas = _import_optional("pandas")
    if pandas is not None:
        codecs.append(PandasDataFrameCodec(pandas.DataFrame))

    polars = _import_optional("polars")
    if polars is not None:
        codecs.append(PolarsDataFrameCodec(polars.DataFrame))

    return codecs
