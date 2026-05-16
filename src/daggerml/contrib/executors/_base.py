from __future__ import annotations

from typing import Any

from daggerml._internal import Runnable


class ExecutorBase:
    """Base class for all executors.

    The runtime owns durable resumable state. Executors receive ``state=None``
    on first launch and the immutable persisted state on later polls. Executors
    return terminal or in-progress result dicts via stdout/return value:

        {"status": "running",    "error": null,  "state": {...}}
        {"status": "succeeded",  "error": null,  "dag_id": "<hex>"}
        {"status": "failed",     "error": "<msg>"}
    """

    name: str = ""
    adapter: str = ""
    execution_status: str | None = None
    cancel_requested_by: str | None = None

    # ------------------------------------------------------------------
    # Subclass interface
    # ------------------------------------------------------------------

    def start(
        self,
        *,
        cache_key: str,
        execution_id: str,
        runnable: Runnable,
        argv_ptr: str,
        remote: dict[str, str],
    ) -> dict[str, Any]:
        """Launch execution and return a result dict.

        For synchronous executors this should return the terminal result
        immediately. For async executors, return the durable resume state in the
        initial ``running`` result.
        """
        raise NotImplementedError

    def poll(
        self,
        *,
        cache_key: str,
        execution_id: str,
        state: dict[str, Any],
        remote: dict[str, str],
    ) -> dict[str, Any]:
        """Check an in-flight job and return a result dict.

        ``state`` is the immutable launch-time state returned by ``start()``.
        Return a terminal result when done, or ``{"status": "running",
        "error": None, "state": ...}`` while still running. Later returned
        state may be ignored by the runtime.
        """
        raise NotImplementedError

    def cleanup(self, *, cache_key: str, execution_id: str, remote: dict[str, str], state: dict[str, Any]) -> None:
        """Optional cleanup hook called after terminal result is handled.

        Default is a no-op.  Subclasses may override to terminate external
        resources (containers, batch jobs, etc.) if needed after the executor
        is known to be done.
        """

    def cancel(
        self, *, cache_key: str, execution_id: str, state: dict[str, Any], remote: dict[str, str]
    ) -> dict[str, Any]:
        self.cleanup(cache_key=cache_key, execution_id=execution_id, remote=remote, state=state)
        return {"status": "cancelled", "error": None}

    # ------------------------------------------------------------------
    # Main dispatch
    # ------------------------------------------------------------------

    @classmethod
    def handle(
        cls,
        *,
        cache_key: str,
        execution_id: str,
        state: dict[str, Any] | None,
        execution_status: str | None,
        cancel_requested_by: str | None,
        runnable: Runnable,
        argv_ptr: str,
        remote: dict[str, str],
    ) -> dict[str, Any]:
        """Call start or poll depending on whether immutable state exists."""
        executor = cls()
        executor.execution_status = execution_status
        executor.cancel_requested_by = cancel_requested_by
        if execution_status == "cancel-requested" and state is not None:
            return executor.cancel(
                cache_key=cache_key,
                execution_id=execution_id,
                state=state,
                remote=remote,
            )
        if state is None:
            return executor.start(
                cache_key=cache_key,
                execution_id=execution_id,
                runnable=runnable,
                argv_ptr=argv_ptr,
                remote=remote,
            )
        return executor.poll(
            cache_key=cache_key,
            execution_id=execution_id,
            state=state,
            remote=remote,
        )
