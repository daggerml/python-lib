from __future__ import annotations

from importlib import metadata
from threading import Lock
from typing import Any

from daggerml._internal import DmlRepoError

ADAPTER_ENTRYPOINT_GROUP = "daggerml.contrib.adapters"

_LOCK = Lock()
_ADAPTER_SPECS: dict[str, Any] = {}
_PLUGINS_LOADED = False


def _entry_points() -> list[metadata.EntryPoint]:
    points = metadata.entry_points()
    result = list(points.select(group=ADAPTER_ENTRYPOINT_GROUP))
    result.sort(key=lambda ep: (ep.name, ep.value))
    return result


def _validate_adapter_spec(spec: Any) -> tuple[str, Any]:
    if not hasattr(spec, "name"):
        raise DmlRepoError("Adapter spec missing required attribute: name")
    if not hasattr(spec, "executable"):
        raise DmlRepoError("Adapter spec missing required attribute: executable")
    if not hasattr(spec, "resolve_runnable"):
        raise DmlRepoError("Adapter spec missing required attribute: resolve_runnable")
    if not hasattr(spec, "send"):
        raise DmlRepoError("Adapter spec missing required callable: send")
    if not hasattr(spec, "cli"):
        raise DmlRepoError("Adapter spec missing required callable: cli")

    name = spec.name
    if not isinstance(name, str) or not name:
        raise DmlRepoError("Adapter spec name must be a non-empty string")
    executable = spec.executable
    if not isinstance(executable, str) or not executable:
        raise DmlRepoError("Adapter spec executable must be a non-empty string")
    if not callable(spec.resolve_runnable):
        raise DmlRepoError("Adapter spec missing required callable: resolve_runnable")
    if not callable(spec.send):
        raise DmlRepoError("Adapter spec missing required callable: send")
    if not callable(spec.cli):
        raise DmlRepoError("Adapter spec missing required callable: cli")
    return name, spec


def register_adapter(spec: Any) -> None:
    name, normalized = _validate_adapter_spec(spec)
    with _LOCK:
        _ADAPTER_SPECS[name] = normalized


def _register_plugin_value(value: Any, *, source: str) -> None:
    try:
        register_adapter(value)
        return
    except DmlRepoError:
        pass

    if isinstance(value, (list, tuple, set)):
        for item in value:
            _register_plugin_value(item, source=source)
        return

    if callable(value):
        _register_plugin_value(value(), source=source)
        return

    raise DmlRepoError(f"Adapter plugin '{source}' returned invalid adapter registration")


def load_adapter_plugins() -> None:
    global _PLUGINS_LOADED
    with _LOCK:
        if _PLUGINS_LOADED:
            return
        entry_points = _entry_points()
    for ep in entry_points:
        source = f"{ep.name} ({ep.value})"
        try:
            loaded = ep.load()
            _register_plugin_value(loaded, source=source)
        except Exception as e:
            raise DmlRepoError(f"Adapter plugin '{source}' failed: {e}") from e
    with _LOCK:
        _PLUGINS_LOADED = True


def get_adapter(name: str) -> Any:
    load_adapter_plugins()
    with _LOCK:
        spec = _ADAPTER_SPECS.get(name)
    if spec is None:
        raise DmlRepoError(f"Adapter '{name}' is not registered")
    return spec


def list_adapters() -> list[str]:
    load_adapter_plugins()
    with _LOCK:
        return sorted(_ADAPTER_SPECS.keys())


def _reset_for_tests() -> None:
    global _PLUGINS_LOADED
    with _LOCK:
        _ADAPTER_SPECS.clear()
        _PLUGINS_LOADED = False
