"""String serialization/deserialization for a small set of DML types.

Supported:
- JSON scalars: None, bool, int, float (finite), str
- JSON containers: list, dict[str, ...]
- daggerml._internal.Ref
- daggerml._internal.types.Uri
- daggerml._internal.types.Error
- daggerml._internal.types.Runnable

Format: JSON text with tagged objects under reserved key "__dml__".
"""

from __future__ import annotations

import json
import math
from typing import Any, Final

from daggerml._internal._db import Ref
from daggerml._internal.types import Error, Runnable, Uri

_DML_TAG: Final[str] = "__dml__"


def dml_dumps(obj: Any) -> str:
    """Serialize a supported object to a stable JSON string."""
    enc = _encode(obj)
    return json.dumps(enc, separators=(",", ":"), sort_keys=True)


def dml_loads(s: str) -> Any:
    """Deserialize a string produced by dml_dumps back to objects."""
    raw = json.loads(s)
    return _decode(raw)


def _encode(obj: Any) -> Any:
    if obj is None or isinstance(obj, (bool, int, str)):
        return obj

    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise TypeError("cannot serialize non-finite float")
        return obj

    if isinstance(obj, Ref):
        return {_DML_TAG: {"t": "Ref", "to": obj.to}}

    if isinstance(obj, Uri):
        return {_DML_TAG: {"t": "Uri", "uri": obj.uri}}

    if isinstance(obj, Error):
        return {
            _DML_TAG: {
                "t": "Error",
                "message": obj.message,
                "origin": obj.origin,
                "type": obj.type,
                "stack": _encode(obj.stack),
            }
        }

    if isinstance(obj, Runnable):
        return {
            _DML_TAG: {
                "t": "Runnable",
                "target": _encode(obj.target),
                "sub": _encode(obj.sub) if obj.sub is not None else None,
                "kwargs": _encode(obj.kwargs),
                "adapter": obj.adapter,
            }
        }

    if isinstance(obj, list):
        return [_encode(v) for v in obj]

    if isinstance(obj, dict):
        if _DML_TAG in obj:
            raise TypeError(f"cannot serialize dict containing reserved key {_DML_TAG!r}")
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if not isinstance(k, str):
                raise TypeError("only dict[str, ...] is supported")
            out[k] = _encode(v)
        return out

    raise TypeError(f"unsupported type for dml_dumps: {type(obj).__name__}")


def _decode(obj: Any) -> Any:
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj

    if isinstance(obj, list):
        return [_decode(v) for v in obj]

    if isinstance(obj, dict):
        if set(obj.keys()) == {_DML_TAG} and isinstance(obj[_DML_TAG], dict):
            tag = obj[_DML_TAG]
            t = tag.get("t")
            if t == "Ref":
                return Ref(tag["to"])
            if t == "Uri":
                return Uri(tag["uri"])
            if t == "Error":
                return Error(
                    message=tag["message"],
                    origin=tag["origin"],
                    type=tag["type"],
                    stack=_decode(tag.get("stack", [])),
                )
            if t == "Runnable":
                target = _decode(tag["target"])
                if not isinstance(target, Uri):
                    raise TypeError("decoded Runnable.target is not a Uri")
                sub_raw = tag.get("sub")
                sub = None if sub_raw is None else _decode(sub_raw)
                if sub is not None and not isinstance(sub, Runnable):
                    raise TypeError("decoded Runnable.sub is not a Runnable")
                kwargs = _decode(tag.get("kwargs", {}))
                if not isinstance(kwargs, dict):
                    raise TypeError("decoded Runnable.kwargs is not a dict")
                return Runnable(target=target, sub=sub, kwargs=kwargs, adapter=tag.get("adapter", ""))
            raise ValueError(f"unknown dml tag type: {t!r}")

        return {k: _decode(v) for k, v in obj.items()}

    raise TypeError(f"unsupported JSON value during decode: {type(obj).__name__}")
