from __future__ import annotations

from typing import Any

import daggerml.api as codec_mod
from daggerml.contrib import adapters as areg
from daggerml.contrib.executors import _base as ereg


def _diag(*, scope: str, code: str, message: str) -> dict[str, str]:
    return {
        "severity": "error",
        "scope": scope,
        "code": code,
        "message": message,
    }


def _fqn(obj: Any) -> str:
    module = getattr(obj, "__module__", None) or type(obj).__module__
    qualname = getattr(obj, "__qualname__", None) or type(obj).__qualname__
    return f"{module}:{qualname}"


def _implements(obj: Any, names: tuple[str, ...]) -> dict[str, bool]:
    return {name: callable(getattr(obj, name, None)) for name in names}


def _registration(
    kind: str, key: str, obj: Any, diagnostics: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    required = {
        "adapter": ("resolve_runnable", "send", "cli"),
        "executor": ("resolve_runnable", "start", "poll", "cleanup", "cancel"),
        "codec": ("can_encode", "encode"),
    }[kind]
    implements = _implements(obj, required)
    missing = [name for name, implemented in implements.items() if not implemented]
    if missing and diagnostics is not None:
        diagnostics.append(
            _diag(
                scope=kind,
                code="required_operation_missing",
                message=f"{key} is missing required operations: {', '.join(missing)}",
            )
        )
    return {
        "key": key,
        "fqn": _fqn(obj),
        "effective": not missing,
        "implements": implements,
    }


def _load_plugins(scope: str, load: Any, diagnostics: list[dict[str, Any]]) -> None:
    try:
        load()
    except Exception as e:
        diagnostics.append(_diag(scope=scope, code="plugin_load_failed", message=str(e)))


def _adapter_status(diagnostics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    _load_plugins("adapter", areg.load_adapter_plugins, diagnostics)
    with areg._LOCK:
        items = sorted(areg._ADAPTER_SPECS.items())
    return [_registration("adapter", name, spec, diagnostics) for name, spec in items]


def _executor_status(diagnostics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    _load_plugins("executor", ereg.load_executor_plugins, diagnostics)
    with ereg._LOCK:
        items = sorted(ereg._EXECUTOR_SPECS.items())
    return [_registration("executor", f"{adapter}:{name}", spec, diagnostics) for (adapter, name), spec in items]


def _codec_status(diagnostics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    try:
        codecs = list(codec_mod.iter_codecs())
    except Exception as e:
        diagnostics.append(_diag(scope="codec", code="plugin_load_failed", message=str(e)))
        codecs = []
    return [_registration("codec", str(order), codec, diagnostics) for order, codec in enumerate(codecs)]


def status() -> dict[str, object]:
    diagnostics: list[dict[str, Any]] = []
    adapters = _adapter_status(diagnostics)
    executors = _executor_status(diagnostics)
    codecs = _codec_status(diagnostics)
    diagnostics.sort(key=lambda item: (item["scope"], item["code"], item["message"]))

    return {
        "schema_version": 0,
        "summary": {
            "has_errors": bool(diagnostics),
            "diagnostic_count": len(diagnostics),
            "adapter_registration_count": len(adapters),
            "adapter_effective_count": sum(item["effective"] for item in adapters),
            "executor_registration_count": len(executors),
            "executor_effective_count": sum(item["effective"] for item in executors),
            "codec_registration_count": len(codecs),
            "codec_effective_count": sum(item["effective"] for item in codecs),
        },
        "adapters": adapters,
        "executors": executors,
        "codecs": codecs,
        "diagnostics": diagnostics,
    }
