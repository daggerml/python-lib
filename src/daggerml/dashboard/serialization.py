"""Safe, bounded JSON projections used by the local dashboard."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit, urlunsplit

from daggerml._core import Ref, Runnable, Uri

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

_SECRET_PARTS = (
    "authorization",
    "credential",
    "password",
    "passwd",
    "secret",
    "session_token",
    "access_key",
    "private_key",
)
_ENV_KEYS = {"env", "environment", "environ"}
_REDACTED = "<redacted>"


def _safe_uri(value: str) -> str:
    """Drop user-info, query strings, and fragments from a URI."""
    try:
        parsed = urlsplit(value)
    except ValueError:
        return value
    if not parsed.scheme:
        return value
    hostname = parsed.hostname or ""
    if parsed.port:
        hostname = f"{hostname}:{parsed.port}"
    return urlunsplit((parsed.scheme, hostname, parsed.path, "", ""))


def redact(value: Any, *, key: str | None = None) -> Any:
    """Recursively remove secrets and environment values from a projection."""
    lowered = (key or "").lower()
    if lowered in _ENV_KEYS or any(part in lowered for part in _SECRET_PARTS):
        return _REDACTED
    if isinstance(value, Mapping):
        return {str(k): redact(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [redact(item) for item in value]
    if isinstance(value, str) and "://" in value:
        return _safe_uri(value)
    return value


def bounded_json(
    value: Any,
    *,
    max_depth: int = 8,
    max_items: int = 200,
    max_string: int = 16_384,
    _depth: int = 0,
) -> Any:
    """Convert DaggerML values to bounded, JSON-compatible structures."""
    if _depth >= max_depth:
        return {"truncated": True, "reason": "max-depth"}
    if isinstance(value, Ref):
        return value.to
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Uri):
        return {"uri": _safe_uri(value.uri)}
    if isinstance(value, Runnable):
        return project_runnable(value, max_depth=max_depth - _depth)
    if dataclasses.is_dataclass(value):
        value = dataclasses.asdict(cast("DataclassInstance", value))
    if isinstance(value, Mapping):
        items = list(value.items())
        out = {
            str(k): bounded_json(
                redact(v, key=str(k)),
                max_depth=max_depth,
                max_items=max_items,
                max_string=max_string,
                _depth=_depth + 1,
            )
            for k, v in items[:max_items]
        }
        if len(items) > max_items:
            out["_truncated"] = {"remaining": len(items) - max_items}
        return out
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        out = [
            bounded_json(
                item,
                max_depth=max_depth,
                max_items=max_items,
                max_string=max_string,
                _depth=_depth + 1,
            )
            for item in value[:max_items]
        ]
        if len(value) > max_items:
            out.append({"truncated": True, "remaining": len(value) - max_items})
        return out
    if isinstance(value, bytes):
        return {"type": "bytes", "size": len(value)}
    if isinstance(value, str):
        safe = redact(value, key=None)
        if len(safe) > max_string:
            return {
                "text": safe[:max_string],
                "truncated": True,
                "total_chars": len(safe),
            }
        return safe
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return repr(value)[:max_string]


def _runnable_dict(value: Runnable | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(value, Runnable):
        return {
            "target": value.target.uri,
            "adapter": value.adapter,
            "kwargs": value.kwargs,
            "sub": value.sub,
        }
    return dict(value)


def project_runnable(value: Runnable | Mapping[str, Any], *, max_depth: int = 16) -> dict[str, Any]:
    """Project a runnable chain, retaining only fields pertinent to each executor."""
    if max_depth <= 0:
        return {"kind": "unknown", "truncated": True}
    raw = _runnable_dict(value)
    target = raw.get("target")
    if isinstance(target, Mapping):
        target = target.get("uri")
    elif isinstance(target, Uri):
        target = target.uri
    target = str(target or "")
    kind = target.rsplit(":", 1)[-1].lower()
    kwargs_value = raw.get("kwargs")
    kwargs: Mapping[str, Any] = kwargs_value if isinstance(kwargs_value, Mapping) else {}
    state_value = raw.get("state")
    state: Mapping[str, Any] = state_value if isinstance(state_value, Mapping) else {}
    if raw.get("adapter") == "dml-lambda-adapter" and any(
        field in kwargs for field in ("image", "cpu", "memory", "gpu")
    ):
        kind = "batch"
    elif kind == "cfn":
        kind = "cloudformation"
    common: dict[str, Any] = {
        "kind": kind or "unknown",
        "target": _safe_uri(target),
        "adapter": str(raw.get("adapter") or ""),
    }
    allowed: dict[str, tuple[str, ...]] = {
        "script": ("fn_name", "script_uri", "pid", "stdout_path", "stderr_path"),
        "docker": ("image", "flags", "container_id"),
        "ssh": ("host", "flags", "env_files"),
        "batch": (
            "lambda_uri",
            "image",
            "cpu",
            "memory",
            "gpu",
            "job_queue",
            "job_id",
            "job_definition",
            "status",
            "attempts",
            "log_stream_name",
        ),
        "cloudformation": ("stack_name", "stack_id", "status", "region"),
    }
    pertinent: dict[str, Any] = {}
    for field in allowed.get(kind, ()):
        if field in kwargs:
            pertinent[field] = kwargs[field]
        elif field in state:
            pertinent[field] = state[field]
        elif field in raw:
            pertinent[field] = raw[field]
    if kind not in allowed:
        pertinent = {
            k: v
            for k, v in {**dict(kwargs), **dict(state)}.items()
            if not (k.lower() in _ENV_KEYS or any(part in k.lower() for part in _SECRET_PARTS))
        }
    common["details"] = bounded_json(redact(pertinent), max_depth=5, max_items=50)
    sub = raw.get("sub")
    if isinstance(sub, (Runnable, Mapping)):
        # SSH is a transparent synchronous transport: durable resume state is
        # returned by the nested executor and therefore pertains to its child.
        if kind == "ssh" and state:
            sub = {**_runnable_dict(sub), "state": state}
        common["sub"] = project_runnable(sub, max_depth=max_depth - 1)
    return common
