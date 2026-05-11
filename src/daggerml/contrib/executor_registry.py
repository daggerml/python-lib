from __future__ import annotations

from importlib import metadata
from threading import Lock
from typing import Any

from daggerml._internal import DmlRepoError

EXECUTOR_ENTRYPOINT_GROUP = "daggerml.contrib.executors"

_LOCK = Lock()
_EXECUTOR_SPECS: dict[tuple[str, str], Any] = {}
_PLUGINS_LOADED = False


def _entry_points() -> list[metadata.EntryPoint]:
    points = metadata.entry_points()
    result = list(points.select(group=EXECUTOR_ENTRYPOINT_GROUP))
    result.sort(key=lambda ep: (ep.name, ep.value))
    return result


def _validate_executor_spec(spec: Any) -> tuple[str, str, Any]:
    if not hasattr(spec, "adapter"):
        raise DmlRepoError("Executor spec missing required attribute: adapter")
    adapter = spec.adapter
    if not isinstance(adapter, str) or not adapter:
        raise DmlRepoError("Executor spec adapter must be a non-empty string")
    if not hasattr(spec, "name"):
        raise DmlRepoError("Executor spec missing required attribute: name")
    name = spec.name
    if not isinstance(name, str) or not name:
        raise DmlRepoError("Executor spec missing required attribute: name")
    if hasattr(spec, "resolve_runnable") and callable(spec.resolve_runnable):
        # Front-end only (resolve_runnable without lifecycle) is valid,
        # but if lifecycle callables are present they must be valid.
        has_start = hasattr(spec, "start")
        has_cleanup = hasattr(spec, "cleanup")
        if has_start or has_cleanup:
            if not has_start or not has_cleanup:
                raise DmlRepoError(
                    "Executor spec with resolve_runnable has partial lifecycle: needs both start and cleanup"
                )
            if not callable(spec.start) or not callable(spec.cleanup):
                raise DmlRepoError("Executor attributes: start, cleanup must be callable")
            if hasattr(spec, "poll") and not callable(spec.poll):
                raise DmlRepoError("Executor defines poll attribute but it is not callable")
        return adapter, name, spec
    # back-end only executor spec must have start and cleanup callables
    if not hasattr(spec, "start") or not hasattr(spec, "cleanup"):
        raise DmlRepoError("Executor spec missing required callables: start, cleanup")
    if not callable(spec.start) or not callable(spec.cleanup):
        raise DmlRepoError("Executor attributes: start, cleanup must be callable")
    if hasattr(spec, "poll") and not callable(spec.poll):
        raise DmlRepoError("Executor defines poll attribute but it is not callable")
    return adapter, name, spec


def register_executor(spec: Any) -> None:
    adapter, name, normalized = _validate_executor_spec(spec)
    with _LOCK:
        _EXECUTOR_SPECS[(adapter, name)] = normalized


def _register_plugin_value(value: Any, *, source: str) -> None:
    try:
        register_executor(value)
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

    raise DmlRepoError(f"Executor plugin '{source}' returned invalid executor registration")


def load_executor_plugins() -> None:
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
            raise DmlRepoError(f"Executor plugin '{source}' failed: {e}") from e
    with _LOCK:
        _PLUGINS_LOADED = True


def get_executor(adapter: str, name: str) -> Any:
    load_executor_plugins()
    with _LOCK:
        spec = _EXECUTOR_SPECS.get((adapter, name))
    if spec is None:
        raise DmlRepoError(f"Executor '{name}' is not registered for adapter '{adapter}'")
    return spec


def list_executors(adapter: str | None = None) -> list[str]:
    load_executor_plugins()
    with _LOCK:
        if adapter is None:
            return sorted(name for _adapter, name in _EXECUTOR_SPECS.keys())
        return sorted(name for _adapter, name in _EXECUTOR_SPECS.keys() if _adapter == adapter)


def _reset_for_tests() -> None:
    global _PLUGINS_LOADED
    with _LOCK:
        _EXECUTOR_SPECS.clear()
        _PLUGINS_LOADED = False
