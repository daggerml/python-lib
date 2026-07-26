from __future__ import annotations

import traceback

from daggerml.contrib.executors._base import ExecutorBase


class LambdaExecutorBase(ExecutorBase):
    adapter = "lambda"

    def start(
        self,
        *,
        cache_key: str,
        execution_id: str,
        runnable: dict[str, object],
        remote: dict[str, str],
        scratch_uri: str,
    ):
        raise NotImplementedError("LambdaExecutorBase.start must be implemented by subclasses")

    @classmethod
    def handler(cls, event, context):
        del context
        try:
            return cls.handle(**dict(event))
        except Exception as e:
            error = f"Lambda handler failed: {e}\n\n{traceback.format_exc()}"
            return {"status": "failed", "error": error, "state": None, "dag_id": None}
