from __future__ import annotations

from threading import Lock
from typing import Any
from warnings import warn

from daggerml.api import DmlRepoError, _entry_points


class ExecutorBase:
    """Base class for all executors.

    The runtime owns durable adapter state. Executors receive ``adapter_state=None``
    on first launch and persisted state on later status checks. Executors
    return terminal or in-progress result dicts via stdout/return value:

        {"status": "running",    "error": null,  "state": {...}, "dag_id": null}
        {"status": "succeeded",  "error": null,  "state": null,  "dag_id": "<hex>"}
        {"status": "failed",     "error": "<msg>", "state": null, "dag_id": null}
    """

    name: str = ""
    adapter: str = ""

    # ------------------------------------------------------------------
    # Subclass interface
    # ------------------------------------------------------------------

    def start(self, cache_key, execution_id, runnable, remote, scratch_uri) -> dict[str, Any]:
        """Launch execution and return a result dict.

        For synchronous executors this should return the terminal result
        immediately. For async executors, return the durable resume state in the
        initial ``running`` result.
        """
        raise NotImplementedError

    def poll(self, cache_key, execution_id, runnable, state, remote, scratch_uri) -> dict[str, Any]:
        """Check an in-flight job and return a result dict.

        ``state`` is the immutable launch-time state returned by ``start()``.
        Return a terminal result when done, or ``{"status": "running",
        "error": None, "state": ..., "dag_id": None}`` while still running.
        Later returned state may be ignored by the runtime.
        """
        raise NotImplementedError

    def gc(self, cache_key, execution_id, remote, scratch_uri, state):
        """Optional cleanup hook called after terminal result is handled.

        Default is a no-op.  Subclasses may override to terminate external
        resources (containers, batch jobs, etc.) if needed after the executor
        is known to be done.
        """

    def cancel(
        self, cache_key, execution_id, runnable, state, remote, scratch_uri, cancel_requested_by, argv_ptr=None
    ) -> dict[str, Any]:
        raise NotImplementedError("This executor does not support cancellation")

    # ------------------------------------------------------------------
    # Main dispatch
    # ------------------------------------------------------------------

    @classmethod
    def handle(
        cls,
        *,
        operation: str,
        cache_key: str,
        execution_id: str,
        remote: dict,
        runnable: dict,
        adapter_state: dict | None = None,
        scratch_uri: str,
        requested_by: str | None = None,
        argv_ref: str | None = None,
    ) -> dict[str, Any]:
        """Dispatch an explicit adapter operation to the executor."""
        if operation not in {"invoke", "cancel"}:
            raise DmlRepoError(f"Unsupported adapter operation: {operation}")
        if adapter_state is not None and not isinstance(adapter_state, dict):
            raise DmlRepoError("adapter_state must be an object or null")
        if operation == "cancel" and (not isinstance(argv_ref, str) or not argv_ref):
            raise DmlRepoError("Cancel operation requires a non-empty argv_ref")
        executor = cls()
        if operation == "cancel":
            result = executor.cancel(
                cache_key=cache_key,
                execution_id=execution_id,
                runnable=runnable,
                state=adapter_state,
                remote=remote,
                scratch_uri=scratch_uri,
                cancel_requested_by=requested_by,
                argv_ptr=argv_ref,
            )
        elif adapter_state is None:
            result = executor.start(
                cache_key=cache_key,
                execution_id=execution_id,
                runnable=runnable,
                remote=remote,
                scratch_uri=scratch_uri,
            )
        else:
            result = executor.poll(
                cache_key=cache_key,
                execution_id=execution_id,
                runnable=runnable,
                state=adapter_state,
                remote=remote,
                scratch_uri=scratch_uri,
            )
        state = result.pop("state", None)
        if isinstance(state, dict):
            result["adapter_state"] = state
        elif not isinstance(result.get("adapter_state"), dict):
            result.pop("adapter_state", None)
        return result


################################################################################
############################## Executor registry ###############################
################################################################################
EXECUTOR_ENTRYPOINT_GROUP = "daggerml.contrib.executors"

_LOCK = Lock()
_EXECUTOR_SPECS: dict[tuple[str, str], Any] = {}
_PLUGINS_LOADED = False


def load_executor_plugins() -> None:
    global _PLUGINS_LOADED
    with _LOCK:
        if _PLUGINS_LOADED:
            return
        for ep in _entry_points(EXECUTOR_ENTRYPOINT_GROUP):
            try:
                loaded = ep.load()
                if (loaded.adapter, loaded.name) in _EXECUTOR_SPECS:
                    warn(
                        f"Duplicate executor plugin for adapter '{loaded.adapter}' and name '{loaded.name}'",
                        RuntimeWarning,
                        stacklevel=2,
                    )
                _EXECUTOR_SPECS[(loaded.adapter, loaded.name)] = loaded
            except Exception as e:
                raise DmlRepoError(f"Executor plugin '{ep.name} ({ep.value})' failed: {e}") from e
        _PLUGINS_LOADED = True


def get_executor(adapter: str, name: str) -> Any:
    load_executor_plugins()
    spec = _EXECUTOR_SPECS.get((adapter, name))
    if spec is None:
        raise DmlRepoError(f"Executor '{name}' is not registered for adapter '{adapter}'")
    return spec


def list_executors(adapter: str) -> list[str]:
    load_executor_plugins()
    return sorted(name for _adapter, name in _EXECUTOR_SPECS.keys() if _adapter == adapter)
