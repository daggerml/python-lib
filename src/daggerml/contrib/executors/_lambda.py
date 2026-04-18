from __future__ import annotations

import traceback

from daggerml._internal.types import Runnable
from daggerml.contrib.adapters import AdapterBase
from daggerml.contrib.executor_state import ExecutionRecord, ExecutionState
from daggerml.contrib.executors._base import ExecutorBase


class LambdaExecutorBase(ExecutorBase):
    adapter = "lambda"

    def start(
        self, *, cache_key: str, state: ExecutionRecord, runnable: Runnable, argv_ptr: str, remote: dict[str, str]
    ) -> None:
        raise NotImplementedError("LambdaExecutorBase.start must be implemented by subclasses")

    def cleanup(self, *, cache_key: str, state: ExecutionRecord) -> None:
        raise NotImplementedError("LambdaExecutorBase.cleanup must be implemented by subclasses")

    @staticmethod
    def _persist_handler_failure(cache_key: str | None, error: str) -> None:
        if not cache_key:
            return
        try:
            es = ExecutionState(cache_key)
            record = es.get()
            if record is None or record["status"] in {"failed", "succeeded", "done"}:
                return
            if record["status"] == "pending" and not es.claim_running():
                record = es.get()
                if record is None or record["status"] in {"failed", "succeeded", "done"}:
                    return
            record = es.get()
            if record is None or record["status"] != "running" or not es.lock():
                return
            try:
                es.mark_failed(error)
            finally:
                es.unlock()
        except Exception:
            return

    @classmethod
    def handler(cls, event, context):
        del context
        cache_key = None
        try:
            argv_ptr, cache_key, runnable, remote = AdapterBase._parse_payload(event)
            result = cls.handle(runnable=runnable, argv_ptr=argv_ptr, cache_key=cache_key, remote=remote)
            return AdapterBase._validate_output(result)
        except Exception as e:
            error = f"Lambda handler failed: {e}\n\n{traceback.format_exc()}"
            cls._persist_handler_failure(cache_key, error)
            return {"status": "failed", "error": error}
