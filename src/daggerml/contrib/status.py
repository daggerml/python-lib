from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

import daggerml.codecs as codec_mod
from daggerml.contrib import adapter_registry as areg
from daggerml.contrib import executor_registry as ereg


def _diag(*, severity: str, scope: str, code: str, message: str, source: dict[str, Any], key: str | None = None):
    return {
        "severity": severity,
        "scope": scope,
        "code": code,
        "message": message,
        "source": source,
        "key": key,
    }


def _source(kind: str, group: str | None = None, name: str | None = None, value: str | None = None):
    return {
        "kind": kind,
        "group": group,
        "name": name,
        "value": value,
    }


def _object_path(obj: Any) -> str | None:
    module = getattr(obj, "__module__", None)
    qualname = getattr(obj, "__qualname__", None)
    if isinstance(module, str) and module and isinstance(qualname, str) and qualname:
        return f"{module}:{qualname}"
    typ = type(obj)
    module = getattr(typ, "__module__", None)
    qualname = getattr(typ, "__qualname__", None)
    if isinstance(module, str) and module and isinstance(qualname, str) and qualname:
        return f"{module}:{qualname}"
    return None


def _match_entry_point(obj: Any, entry_points: list[Any]) -> dict[str, Any] | None:
    path = _object_path(obj)
    if path is None:
        return None
    for ep in entry_points:
        if ep.value == path:
            return _source("entry_point", getattr(ep, "group", None) or None, ep.name, ep.value)
    return None


def _match_codec_error_source(message: str, entry_points: list[Any]) -> dict[str, Any]:
    match = re.search(r"Literal codec plugin '([^ ]+) \(([^)]+)\)' failed:", message)
    if match is None:
        return _source("none")
    name, value = match.groups()
    for ep in entry_points:
        if ep.name == name and ep.value == value:
            return _source("entry_point", getattr(ep, "group", None) or None, ep.name, ep.value)
    return _source("entry_point", codec_mod.LITERAL_CODEC_ENTRYPOINT_GROUP, name, value)


def _fqn(obj: Any) -> str:
    path = _object_path(obj)
    if path is not None:
        return path
    return f"{type(obj).__module__}:{type(obj).__qualname__}"


def _implements(kind: str, obj: Any):
    if kind == "adapter":
        return {
            "resolve_runnable": callable(getattr(obj, "resolve_runnable", None)),
            "send": callable(getattr(obj, "send", None)),
            "cli": callable(getattr(obj, "cli", None)),
        }
    if kind == "executor":
        return {
            "resolve_runnable": callable(getattr(obj, "resolve_runnable", None)),
            "start": callable(getattr(obj, "start", None)),
            "poll": callable(getattr(obj, "poll", None)),
            "cleanup": callable(getattr(obj, "cleanup", None)),
        }
    return {
        "can_encode": callable(getattr(obj, "can_encode", None)),
        "encode": callable(getattr(obj, "encode", None)),
    }


def _registration(kind: str, key: str, obj: Any, *, effective: bool):
    return {
        "key": key,
        "fqn": _fqn(obj),
        "effective": effective,
        "implements": _implements(kind, obj),
    }


def _collect_adapter_specs(
    value: Any, *, source: dict[str, Any], out: list[tuple[str, Any, dict[str, Any]]], diagnostics: list[dict[str, Any]]
):
    try:
        name, spec = areg._validate_adapter_spec(value)
    except Exception:
        pass
    else:
        out.append((name, spec, source))
        return

    if isinstance(value, (list, tuple, set)):
        for item in value:
            _collect_adapter_specs(item, source=source, out=out, diagnostics=diagnostics)
        return

    if callable(value):
        try:
            produced = value()
        except Exception as e:
            diagnostics.append(
                _diag(
                    severity="error",
                    scope="adapter",
                    code="introspection_failed",
                    message=f"Adapter plugin callable failed: {e}",
                    source=source,
                )
            )
            return
        _collect_adapter_specs(produced, source=source, out=out, diagnostics=diagnostics)
        return

    diagnostics.append(
        _diag(
            severity="error",
            scope="adapter",
            code="registration_invalid",
            message="Adapter plugin returned invalid adapter registration",
            source=source,
        )
    )


def _collect_executor_specs(
    value: Any, *, source: dict[str, Any], out: list[tuple[str, Any, dict[str, Any]]], diagnostics: list[dict[str, Any]]
):
    try:
        adapter, name, spec = ereg._validate_executor_spec(value)
    except Exception:
        pass
    else:
        out.append((f"{adapter}:{name}", spec, source))
        return

    if isinstance(value, (list, tuple, set)):
        for item in value:
            _collect_executor_specs(item, source=source, out=out, diagnostics=diagnostics)
        return

    if callable(value):
        try:
            produced = value()
        except Exception as e:
            diagnostics.append(
                _diag(
                    severity="error",
                    scope="executor",
                    code="introspection_failed",
                    message=f"Executor plugin callable failed: {e}",
                    source=source,
                )
            )
            return
        _collect_executor_specs(produced, source=source, out=out, diagnostics=diagnostics)
        return

    diagnostics.append(
        _diag(
            severity="error",
            scope="executor",
            code="registration_invalid",
            message="Executor plugin returned invalid executor registration",
            source=source,
        )
    )


def _collect_codec_specs(
    value: Any, *, source: dict[str, Any], out: list[tuple[int, Any, dict[str, Any]]], diagnostics: list[dict[str, Any]]
):
    if codec_mod._is_codec(value):
        out.append((0, value, source))
        return
    if isinstance(value, tuple) and len(value) == 2 and isinstance(value[1], int) and codec_mod._is_codec(value[0]):
        out.append((value[1], value[0], source))
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _collect_codec_specs(item, source=source, out=out, diagnostics=diagnostics)
        return
    diagnostics.append(
        _diag(
            severity="error",
            scope="codec",
            code="registration_invalid",
            message="Literal codec plugin returned invalid codec registration",
            source=source,
        )
    )


def _adapter_status(diagnostics: list[dict[str, Any]]):
    with areg._LOCK:
        current = dict(areg._ADAPTER_SPECS)
    entry_points = areg._entry_points()
    candidates: list[tuple[str, Any, dict[str, Any]]] = []

    for name, obj in current.items():
        candidates.append((name, obj, _match_entry_point(obj, entry_points) or _source("runtime")))

    for ep in entry_points:
        source = _source("entry_point", areg.ADAPTER_ENTRYPOINT_GROUP, ep.name, ep.value)
        try:
            loaded = ep.load()
        except Exception as e:
            diagnostics.append(
                _diag(
                    severity="error",
                    scope="adapter",
                    code="entry_point_load_failed",
                    message=f"Adapter plugin '{ep.name} ({ep.value})' failed: {e}",
                    source=source,
                )
            )
            continue
        loaded_specs: list[tuple[str, Any, dict[str, Any]]] = []
        _collect_adapter_specs(loaded, source=source, out=loaded_specs, diagnostics=diagnostics)
        candidates.extend(loaded_specs)

    grouped: dict[str, list[int]] = defaultdict(list)
    for idx, (key, _obj, _src) in enumerate(candidates):
        grouped[key].append(idx)

    effective_idx = {indexes[-1] for indexes in grouped.values()}
    for key, indexes in grouped.items():
        if len(indexes) > 1:
            diagnostics.append(
                _diag(
                    severity="warning",
                    scope="adapter",
                    code="duplicate_key",
                    message=f"Multiple adapter registrations found for key '{key}'",
                    source=_source("runtime"),
                    key=key,
                )
            )

    registrations = [
        _registration("adapter", key, obj, effective=idx in effective_idx)
        for idx, (key, obj, _source) in enumerate(candidates)
    ]
    return registrations


def _executor_status(diagnostics: list[dict[str, Any]]):
    with ereg._LOCK:
        current = dict(ereg._EXECUTOR_SPECS)
    entry_points = ereg._entry_points()
    candidates: list[tuple[str, Any, dict[str, Any]]] = []

    for (adapter, name), obj in current.items():
        key = f"{adapter}:{name}"
        candidates.append((key, obj, _match_entry_point(obj, entry_points) or _source("runtime")))

    for ep in entry_points:
        source = _source("entry_point", ereg.EXECUTOR_ENTRYPOINT_GROUP, ep.name, ep.value)
        try:
            loaded = ep.load()
        except Exception as e:
            diagnostics.append(
                _diag(
                    severity="error",
                    scope="executor",
                    code="entry_point_load_failed",
                    message=f"Executor plugin '{ep.name} ({ep.value})' failed: {e}",
                    source=source,
                )
            )
            continue
        loaded_specs: list[tuple[str, Any, dict[str, Any]]] = []
        _collect_executor_specs(loaded, source=source, out=loaded_specs, diagnostics=diagnostics)
        candidates.extend(loaded_specs)

    grouped: dict[str, list[int]] = defaultdict(list)
    for idx, (key, _obj, _src) in enumerate(candidates):
        grouped[key].append(idx)

    effective_idx = {indexes[-1] for indexes in grouped.values()}
    for key, indexes in grouped.items():
        if len(indexes) > 1:
            diagnostics.append(
                _diag(
                    severity="warning",
                    scope="executor",
                    code="duplicate_key",
                    message=f"Multiple executor registrations found for key '{key}'",
                    source=_source("runtime"),
                    key=key,
                )
            )

    registrations = [
        _registration("executor", key, obj, effective=idx in effective_idx)
        for idx, (key, obj, _source) in enumerate(candidates)
    ]
    return registrations


def _codec_status(diagnostics: list[dict[str, Any]]):
    entry_points = codec_mod._entry_points()
    try:
        codec_mod.ensure_literal_codec_plugins_loaded()
    except Exception as e:
        message = str(e)
        diagnostics.append(
            _diag(
                severity="error",
                scope="codec",
                code="entry_point_load_failed",
                message=message,
                source=_match_codec_error_source(message, entry_points),
            )
        )

    with codec_mod._lock:
        codec_items = list(codec_mod._literal_codecs)

    registrations = []
    for order, (priority, _seq, obj) in enumerate(codec_items):
        type_name = getattr(type(obj), "__qualname__", type(obj).__name__)
        registrations.append(
            _registration(
                "codec",
                f"{priority}:{order}:{type_name}",
                obj,
                effective=True,
            )
        )

    return registrations


def status() -> dict[str, object]:
    diagnostics: list[dict[str, Any]] = []
    adapters = _adapter_status(diagnostics)
    executors = _executor_status(diagnostics)
    codecs = _codec_status(diagnostics)
    diagnostics.sort(key=lambda item: (item["scope"], item["code"], item["message"]))

    return {
        "schema_version": 0,
        "summary": {
            "has_errors": any(item["severity"] == "error" for item in diagnostics),
            "diagnostic_count": len(diagnostics),
            "adapter_registration_count": len(adapters),
            "adapter_effective_count": sum(1 for item in adapters if item["effective"]),
            "executor_registration_count": len(executors),
            "executor_effective_count": sum(1 for item in executors if item["effective"]),
            "codec_registration_count": len(codecs),
            "codec_effective_count": len(codecs),
        },
        "adapters": adapters,
        "executors": executors,
        "codecs": codecs,
        "diagnostics": diagnostics,
    }
