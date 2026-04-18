from __future__ import annotations

from typing import Any

from daggerml._internal.types import DmlRepoError, Runnable
from daggerml.contrib.executor_state import ExecutionRecord, ExecutionState


class ExecutorBase:
    """Base class for all executors.

    Subclasses must set ``name`` and ``adapter`` class attributes and implement
    ``start``, ``poll``, and ``cleanup``.
    """

    name: str = ""
    adapter: str = ""

    def start(
        self,
        *,
        cache_key: str,
        state: ExecutionRecord,
        runnable: Runnable,
        argv_ptr: str,
        remote: dict[str, str],
    ) -> None:
        """Launch execution.  Called when status is ``pending``."""
        raise NotImplementedError

    def poll(self, *, cache_key: str, state: ExecutionRecord) -> None:
        """Check in-flight execution.  Called when status is ``running``.

        Default is no-op (suitable for supervisor-backed executors).
        """

    def cleanup(self, *, cache_key: str, state: ExecutionRecord) -> None:
        """Release resources.  Called when status is ``succeeded`` or ``failed``."""
        raise NotImplementedError

    @classmethod
    def handle(
        cls,
        *,
        cache_key: str,
        runnable: Runnable,
        argv_ptr: str,
        remote: dict[str, str],
    ) -> dict[str, Any]:
        """Read state, dispatch one bounded lifecycle step, return ``{status, error}``."""
        es = ExecutionState(cache_key)
        record = es.get()
        if record is None:
            raise DmlRepoError(f"ExecutorBase.handle: no state record for cache_key={cache_key!r}")

        executor = cls()
        status = record["status"]

        if status == "pending":
            if not es.claim_running():
                record = es.get() or record
                if record["status"] == "running":
                    return _result(record)
                if record["status"] in ("succeeded", "failed"):
                    executor.cleanup(cache_key=cache_key, state=record)
                    return _result(record)
                raise DmlRepoError(f"ExecutorBase.handle: unexpected execution status {record['status']!r}")
            record = es.get() or record
            executor.start(
                cache_key=cache_key,
                state=record,
                runnable=runnable,
                argv_ptr=argv_ptr,
                remote=remote,
            )
            record = es.get() or record
            if record["status"] in ("succeeded", "failed"):
                executor.cleanup(cache_key=cache_key, state=record)
            return _result(record)

        if status == "running":
            executor.poll(cache_key=cache_key, state=record)
            record = es.get() or record
            if record["status"] in ("succeeded", "failed"):
                executor.cleanup(cache_key=cache_key, state=record)
            return _result(record)

        if status in ("succeeded", "failed"):
            executor.cleanup(cache_key=cache_key, state=record)
            return _result(record)

        raise DmlRepoError(f"ExecutorBase.handle: unexpected execution status {status!r}")


def _result(record: ExecutionRecord) -> dict[str, Any]:
    return {"status": record["status"], "error": record.get("error")}
