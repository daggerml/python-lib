from __future__ import annotations

import json
from dataclasses import dataclass

from daggerml import Dml, Error
from daggerml.contrib.executor_state import LocalState
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


@dataclass
class CfnExecutor(ExecutorBase):
    name = "cfn"
    adapter = "local"
    state_class = LocalState

    @staticmethod
    def _client():
        return get_client("cloudformation")

    @classmethod
    def _tmpdag(cls, argv_ptr):
        try:
            with Dml.temporary() as dml:
                with dml.new(argv_ptr=argv_ptr) as dag:
                    yield dag
                dag.cache()
        except Exception:
            pass

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

    @classmethod
    def _fail_dag(cls, argv_ptr, message):
        with cls._tmpdag(argv_ptr) as dag:
            dag.commit(Error(message, origin="cfn-executor", type="runtimeerror"))

    @classmethod
    def start(cls, *, runnable, argv_ptr, cache_key, remote, state):
        with Dml.temporary() as dml:
            with dml.new(argv_ptr=argv_ptr) as dag:
                name, template, params = dag.argv[1:4].value()
        state.put_if_absent(state.init_record(status="pending"))
        client = cls._client()
        old_stack_id = None
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
            else:
                resp = client.update_stack(
                    StackName=name,
                    TemplateBody=json.dumps(template),
                    Parameters=[{"ParameterKey": k, "ParameterValue": v} for k, v in params.items()],
                    Capabilities=["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"],
                )
        except Exception as e:
            if "No updates are to be performed" not in str(e):
                raise
            return_poll = True
        stack_id = resp["StackId"]
        state.update(
            state.set_executor_metadata(cls.name, data={"stack_name": name, "stack_id": stack_id, "argv_ptr": argv_ptr})
        )
        if return_poll:
            return cls.poll(state)
        return {"status": "pending", "error": None}

    @classmethod
    def poll(cls, state):
        metadata = state.get_executor_metadata(cls.name)
        stack_name = metadata["stack_name"]
        try:
            stacks = cls._client().describe_stacks(StackName=stack_name)["Stacks"]
        except Exception:
            return {"status": "running", "error": None}
        if not stacks:
            error = f"Stack not found: {stack_name}"
            cls._fail_dag(metadata["argv_ptr"], error)
            return {"status": "failed", "error": error}
        stack = stacks[0]
        raw_status = stack["StackStatus"]
        if raw_status in TERMINAL_SUCCESS_STATUSES:
            outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
            metadata["outputs"] = outputs
            cls._commit_dag(metadata, stack, outputs)
            return {"status": "succeeded", "error": None}
        if raw_status in TERMINAL_FAILED_STATUSES:
            error = f"Stack {stack_name} failed: {raw_status}"
            try:
                events = cls._client().describe_stack_events(StackName=stack_name)["StackEvents"]
                reasons = [e["ResourceStatusReason"] for e in events if "ResourceStatusReason" in e]
                if reasons:
                    error = f"{error}\n{chr(10).join(reasons)}"
            except Exception:
                pass
            cls._fail_dag(metadata["argv_ptr"], error)
            return {"status": "failed", "error": error}
        return {"status": "running", "error": None}

    @classmethod
    def gc(cls, state):
        state.delete()
