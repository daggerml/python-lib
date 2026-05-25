"""String serialization/deserialization for a small set of DML types.

Supported:
- JSON scalars: None, bool, int, float (finite), str
- JSON containers: list, dict[str, ...]
- daggerml._internal.Ref
- daggerml._internal.types.Uri
- daggerml._internal.types.Error
- daggerml._internal.types.Runnable

Format: every serialized value uses an explicit envelope:
- ["scalar", ...]
- ["list", [...]]
- ["dict", {...}]
- ["ref", "datum:..."]
- ["uri", "file:///"]
- ["error", {...}]
- ["runnable", {...}]

This avoids reserved user-data keys inside plain dictionaries because user
`dict` values are always nested under the envelope payload.
"""

from __future__ import annotations

import json
import math
from typing import Any, Final

from daggerml._internal._db import Ref
from daggerml._internal.types import Error, Runnable, Uri

_TYPE_SCALAR: Final[str] = "scalar"
_TYPE_LIST: Final[str] = "list"
_TYPE_DICT: Final[str] = "dict"
_TYPE_REF: Final[str] = "ref"
_TYPE_URI: Final[str] = "uri"
_TYPE_ERROR: Final[str] = "error"
_TYPE_RUNNABLE: Final[str] = "runnable"


def dml_dumps(obj: Any) -> str:
    """Serialize a supported object to a stable JSON string."""
    return json.dumps(_encode(obj), separators=(",", ":"), sort_keys=True)


def dml_loads(s: str) -> Any:
    """Deserialize a string produced by dml_dumps back to objects."""
    return _decode(json.loads(s))


def _is_scalar(obj: Any) -> bool:
    return obj is None or isinstance(obj, (bool, int, float, str))


def _encode(obj: Any) -> list[Any]:
    if _is_scalar(obj):
        if isinstance(obj, float) and not math.isfinite(obj):
            raise TypeError("cannot serialize non-finite float")
        return [_TYPE_SCALAR, obj]

    if isinstance(obj, Ref):
        return [_TYPE_REF, obj.to]

    if isinstance(obj, Uri):
        return [_TYPE_URI, obj.uri]

    if isinstance(obj, Error):
        return [
            _TYPE_ERROR,
            {
                "message": _encode(obj.message),
                "origin": _encode(obj.origin),
                "type": _encode(obj.type),
                "stack": _encode(obj.stack),
            },
        ]

    if isinstance(obj, Runnable):
        return [
            _TYPE_RUNNABLE,
            {
                "target": _encode(obj.target),
                "sub": _encode(obj.sub),
                "kwargs": _encode(obj.kwargs),
                "adapter": _encode(obj.adapter),
            },
        ]

    if isinstance(obj, list):
        return [_TYPE_LIST, [_encode(v) for v in obj]]

    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if not isinstance(k, str):
                raise TypeError("only dict[str, ...] is supported")
            out[k] = _encode(v)
        return [_TYPE_DICT, out]

    raise TypeError(f"unsupported type for dml_dumps: {type(obj).__name__}")


def _decode(obj: Any) -> Any:
    if not isinstance(obj, list):
        raise TypeError(f"expected DML envelope array, got {type(obj).__name__}")
    if len(obj) != 2:
        raise ValueError("expected DML envelope array of length 2")

    type_name, value = obj

    if type_name == _TYPE_SCALAR:
        if not _is_scalar(value):
            raise TypeError("scalar envelope must carry None, bool, int, float, or str")
        if isinstance(value, float) and not math.isfinite(value):
            raise TypeError("scalar envelope must carry a finite float")
        return value

    if type_name == _TYPE_REF:
        if not isinstance(value, str):
            raise TypeError("ref envelope must carry a string")
        return Ref(value)

    if type_name == _TYPE_URI:
        if not isinstance(value, str):
            raise TypeError("uri envelope must carry a string")
        return Uri(value)

    if type_name == _TYPE_LIST:
        if not isinstance(value, list):
            raise TypeError("list envelope must carry a list")
        return [_decode(v) for v in value]

    if type_name == _TYPE_DICT:
        if not isinstance(value, dict):
            raise TypeError("dict envelope must carry a dict")
        out: dict[str, Any] = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise TypeError("dict envelope keys must be strings")
            out[k] = _decode(v)
        return out

    if type_name == _TYPE_ERROR:
        if not isinstance(value, dict):
            raise TypeError("error envelope must carry a dict")
        return Error(
            message=_decode(value["message"]),
            origin=_decode(value["origin"]),
            type=_decode(value["type"]),
            stack=_decode(value["stack"]),
        )

    if type_name == _TYPE_RUNNABLE:
        if not isinstance(value, dict):
            raise TypeError("runnable envelope must carry a dict")
        target = _decode(value["target"])
        sub = _decode(value["sub"])
        kwargs = _decode(value["kwargs"])
        adapter = _decode(value["adapter"])
        if not isinstance(target, Uri):
            raise TypeError("decoded Runnable.target is not a Uri")
        if sub is not None and not isinstance(sub, Runnable):
            raise TypeError("decoded Runnable.sub is not a Runnable")
        if not isinstance(kwargs, dict):
            raise TypeError("decoded Runnable.kwargs is not a dict")
        if not isinstance(adapter, str):
            raise TypeError("decoded Runnable.adapter is not a string")
        return Runnable(target=target, sub=sub, kwargs=kwargs, adapter=adapter)

    raise ValueError(f"unknown DML envelope type: {type_name!r}")
