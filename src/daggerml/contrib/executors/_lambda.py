from __future__ import annotations

import traceback

from daggerml._internal.execution_context import execution_context
from daggerml._internal.types import Runnable
from daggerml.contrib.adapters import AdapterBase
from daggerml.contrib.executors._base import ExecutorBase


class LambdaExecutorBase(ExecutorBase):
    adapter = "lambda"

    def start(
        self,
        *,
        cache_key: str,
        execution_id: str,
        runnable: Runnable,
        argv_ptr: str,
        remote: dict[str, str],
    ):
        raise NotImplementedError("LambdaExecutorBase.start must be implemented by subclasses")

    @classmethod
    def handler(cls, event, context):
        del context
        try:
            argv_ptr, cache_key, execution_id, runnable, remote, state, execution_status, cancel_requested_by = (
                AdapterBase._parse_payload(event)
            )
            with execution_context(execution_id, cache_key):
                result = cls.handle(
                    runnable=runnable,
                    argv_ptr=argv_ptr,
                    cache_key=cache_key,
                    execution_id=execution_id,
                    remote=remote,
                    state=state,
                    execution_status=execution_status,
                    cancel_requested_by=cancel_requested_by,
                )
            return AdapterBase._validate_output(result)
        except Exception as e:
            error = f"Lambda handler failed: {e}\n\n{traceback.format_exc()}"
            return {"status": "failed", "error": error}
