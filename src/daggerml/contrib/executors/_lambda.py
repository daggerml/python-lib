from __future__ import annotations

import traceback
from dataclasses import dataclass
from typing import Any

from daggerml._internal.types import DmlRepoError
from daggerml.contrib.adapters import AdapterBase
from daggerml.contrib.executor_state import DynamoState
from daggerml.contrib.executors._base import ExecutorBase


@dataclass
class LambdaExecutorBase(ExecutorBase):
    adapter = "lambda"
    state_class = DynamoState

    @classmethod
    def start(cls, runnable, argv_ptr, cache_key, remote, state):
        raise NotImplementedError("LambdaExecutorBase does not implement start, it must be implemented by subclasses")

    @classmethod
    def poll(cls, state):
        raise NotImplementedError("LambdaExecutorBase does not implement poll, it must be implemented by subclasses")

    @classmethod
    def gc(cls, state):
        raise NotImplementedError("LambdaExecutorBase does not implement gc, it must be implemented by subclasses")

    @staticmethod
    def _child_comms(state) -> dict[str, Any]:
        if isinstance(state, DynamoState):
            return {"kind": "dynamo", "spec": {"table_name": state.table_name}}
        raise DmlRepoError("batch executor requires dynamo state backend")

    @staticmethod
    def _release_lease(state):
        record = state.get()
        if record is None:
            return
        state.update(state.update_status(status=record["status"], error=record["error"]))

    @classmethod
    def _handle_once(cls, *, runnable, argv_ptr, cache_key, remote):
        with cls.state_class(cache_key).lock() as state:
            if state is None:
                return {"status": "running", "error": None}
            try:
                current = state.get()
                if current is None:
                    result = cls.start(
                        runnable=runnable,
                        argv_ptr=argv_ptr,
                        cache_key=cache_key,
                        remote=remote,
                        state=state,
                    )
                elif current.get("status") in {"succeeded", "failed", "canceled"}:
                    result = {"status": current["status"], "error": current.get("error")}
                else:
                    result = cls.poll(state=state)
                if result.get("status") in {"succeeded", "failed", "canceled"}:
                    cls.gc(state=state)
                return result
            finally:
                cls._release_lease(state)

    @classmethod
    def handler(cls, event, context):
        del context
        try:
            argv_ptr, cache_key, runnable, remote, _comms = AdapterBase._parse_payload(event)
            result = cls._handle_once(runnable=runnable, argv_ptr=argv_ptr, cache_key=cache_key, remote=remote)
            return AdapterBase._validate_output(result)
        except Exception as e:
            return {"status": "failed", "error": f"Lambda handler failed: {e}\n\n{traceback.format_exc()}"}
