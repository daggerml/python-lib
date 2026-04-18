from __future__ import annotations

import json
from contextlib import contextmanager

from daggerml import Dml
from daggerml._internal.types import Runnable
from daggerml.contrib.executor_state import ExecutionRecord, ExecutionState
from daggerml.contrib.executors._base import ExecutorBase
from daggerml.util import get_client

TERMINAL_FAILED_STATUSES = {
    "CREATE_FAILED",
    "ROLLBACK_COMPLETE",
    "ROLLBACK_FAILED",
    "DELETE_FAILED",
    "UPDATE_ROLLBACK_COMPLETE",
    "UPDATE_ROLLBACK_FAILED",
}
TERMINAL_SUCCESS_STATUSES = {"CREATE_COMPLETE", "UPDATE_COMPLETE"}


class CfnExecutor(ExecutorBase):
    name = "cfn"
    adapter = "local"

    @staticmethod
    def _client():
        return get_client("cloudformation")

    @classmethod
    @contextmanager
    def _tmpdag(cls, argv_ptr):
        with Dml.temporary() as dml:
            with dml.new(argv_ptr=argv_ptr) as dag:
                yield dag

    @classmethod
    def _commit_dag(cls, metadata, stack, outputs):
        argv_ptr = metadata.get("argv_ptr")
        with cls._tmpdag(argv_ptr) as dag:
            for k, v in outputs.items():
                dag[k] = v
            dag.stack_id = stack["StackId"]
            dag.stack_name = metadata["stack_name"]
            dag.outputs = outputs
            dag.commit(dag.outputs)
            return dag.ref.id()

    def start(
        self, *, cache_key: str, state: ExecutionRecord, runnable: Runnable, argv_ptr: str, remote: dict[str, str]
    ) -> None:
        es = ExecutionState(cache_key)
        with Dml.temporary() as dml_inst:
            with dml_inst.new(argv_ptr=argv_ptr) as dag:
                name, template, params = dag.argv[1:4].value()

        client = self._client()
        old_stack_id = None
        stack_id = None
        return_poll = False
        try:
            stacks = client.describe_stacks(StackName=name)["Stacks"]
            old_stack_id = stacks[0]["StackId"] if stacks else None
        except Exception:
            pass
        try:
            if old_stack_id is None:
                resp = client.create_stack(
                    StackName=name,
                    TemplateBody=json.dumps(template),
                    Parameters=[{"ParameterKey": k, "ParameterValue": v} for k, v in params.items()],
                    Capabilities=["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"],
                )
                stack_id = resp["StackId"]
            else:
                resp = client.update_stack(
                    StackName=name,
                    TemplateBody=json.dumps(template),
                    Parameters=[{"ParameterKey": k, "ParameterValue": v} for k, v in params.items()],
                    Capabilities=["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"],
                )
                stack_id = resp["StackId"]
        except Exception as e:
            if "No updates are to be performed" not in str(e):
                raise
            stack_id = old_stack_id
            return_poll = True

        assert es.lock()
        try:
            es.update_metadata(
                {
                    self.name: {"stack_name": name, "stack_id": stack_id, "argv_ptr": argv_ptr},
                }
            )
        finally:
            es.unlock()

        if return_poll:
            self.poll(cache_key=cache_key, state=es.get() or state)

    def poll(self, *, cache_key: str, state: ExecutionRecord) -> None:
        meta = (state.get("metadata") or {}).get(self.name, {})
        stack_name = meta.get("stack_name")
        if not stack_name:
            return
        try:
            stacks = self._client().describe_stacks(StackName=stack_name)["Stacks"]
        except Exception:
            return
        if not stacks:
            error = f"Stack not found: {stack_name}"
            es = ExecutionState(cache_key)
            if es.lock():
                try:
                    es.mark_failed(error)
                finally:
                    es.unlock()
            return
        stack = stacks[0]
        raw_status = stack["StackStatus"]
        if raw_status in TERMINAL_SUCCESS_STATUSES:
            outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
            dag_id = self._commit_dag(meta, stack, outputs)
            es = ExecutionState(cache_key)
            if es.lock():
                try:
                    es.mark_succeeded(dag_id)
                finally:
                    es.unlock()
            return
        if raw_status in TERMINAL_FAILED_STATUSES:
            error = f"Stack {stack_name} failed: {raw_status}"
            try:
                events = self._client().describe_stack_events(StackName=stack_name)["StackEvents"]
                reasons = [e["ResourceStatusReason"] for e in events if "ResourceStatusReason" in e]
                if reasons:
                    error = f"{error}\n{chr(10).join(reasons)}"
            except Exception:
                pass
            es = ExecutionState(cache_key)
            if es.lock():
                try:
                    es.mark_failed(error)
                finally:
                    es.unlock()

    def cleanup(self, *, cache_key: str, state: ExecutionRecord) -> None:
        pass  # CFN stacks are not cleaned up on completion
