from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any
from uuid import uuid4

from daggerml import new, temporary
from daggerml._internal import Runnable
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
    def _tmpdag(cls, argv_ptr, *, remote_root: str):
        with temporary(remote_uri=remote_root, name=f"cfn-{uuid4().hex}") as dml:
            with new(dml=dml, argv_ptr=argv_ptr) as dag:
                yield dag

    @classmethod
    def _commit_dag(cls, metadata, stack, outputs, *, remote_root: str):
        argv_ptr = metadata.get("argv_ptr")
        with cls._tmpdag(argv_ptr, remote_root=remote_root) as dag:
            for k, v in outputs.items():
                dag[k] = v
            dag.stack_id = stack["StackId"]
            dag.stack_name = metadata["stack_name"]
            dag.outputs = outputs
            dag.commit(dag.outputs)
            return dag.ref.id()

    def start(
        self,
        *,
        cache_key: str,
        execution_id: str,
        runnable: Runnable,
        argv_ptr: str,
        remote: dict[str, str],
    ) -> dict[str, Any]:
        del runnable
        with temporary(remote_uri=remote["root"], name=f"cfn-{execution_id}") as dml_inst:
            with new(dml=dml_inst, argv_ptr=argv_ptr) as dag:
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

        job_state = {"stack_name": name, "stack_id": stack_id, "argv_ptr": argv_ptr}

        if return_poll:
            return self.poll(cache_key=cache_key, execution_id=execution_id, state=job_state, remote=remote)
        return {"status": "running", "error": None, "state": job_state}

    def poll(
        self,
        *,
        cache_key: str,
        execution_id: str,
        state: dict[str, Any],
        remote: dict[str, str],
    ) -> dict[str, Any]:
        del cache_key, execution_id
        stack_name = state.get("stack_name")
        if not stack_name:
            return {"status": "failed", "error": "cfn poll: missing stack_name in job state"}
        try:
            stacks = self._client().describe_stacks(StackName=stack_name)["Stacks"]
        except Exception:
            return {"status": "running", "error": None, "state": state}
        if not stacks:
            return {"status": "failed", "error": f"Stack not found: {stack_name}"}
        stack = stacks[0]
        raw_status = stack["StackStatus"]
        if raw_status in TERMINAL_SUCCESS_STATUSES:
            outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
            dag_id = self._commit_dag(state, stack, outputs, remote_root=remote["root"])
            return {"status": "succeeded", "error": None, "dag_id": dag_id}
        if raw_status in TERMINAL_FAILED_STATUSES:
            error = f"Stack {stack_name} failed: {raw_status}"
            try:
                events = self._client().describe_stack_events(StackName=stack_name)["StackEvents"]
                reasons = [e["ResourceStatusReason"] for e in events if "ResourceStatusReason" in e]
                if reasons:
                    error = f"{error}\n{chr(10).join(reasons)}"
            except Exception:
                pass
            return {"status": "failed", "error": error}
        return {"status": "running", "error": None, "state": state}

    def cancel(
        self, *, cache_key: str, execution_id: str, state: dict[str, Any], remote: dict[str, str]
    ) -> dict[str, Any]:
        del cache_key, execution_id, remote
        stack_name = state.get("stack_name")
        if isinstance(stack_name, str) and stack_name:
            client = self._client()
            try:
                client.cancel_update_stack(StackName=stack_name)
            except Exception:
                try:
                    client.delete_stack(StackName=stack_name)
                except Exception:
                    pass
        return {"status": "cancel-detached", "error": None}
