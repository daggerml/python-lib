from __future__ import annotations

import json
import os
from typing import Any

from daggerml import Uri
from daggerml._internal.exec_state import ExecutionState
from daggerml._internal.types import DmlRepoError, Runnable
from daggerml.contrib.adapters import AdapterBase
from daggerml.contrib.executors._lambda import LambdaExecutorBase
from daggerml.util import get_client

PENDING_BATCH_STATUSES = {"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"}
DEFAULT_VCPU = 1
DEFAULT_MEMORY = 16 * 1024
DEFAULT_GPU = 0

_ADAPTER_IO_NAME = "lambda:batch"


class BatchExecutor(LambdaExecutorBase):
    name = "batch"

    @staticmethod
    def _string(name: str, value: Any) -> str:
        if not isinstance(value, str) or not value:
            raise DmlRepoError(f"batch executor {name} must be a non-empty string")
        return value

    @staticmethod
    def _int(name: str, value: Any, *, default: int, min_value: int = 0) -> int:
        if value is None:
            return default
        if not isinstance(value, int) or value < min_value:
            raise DmlRepoError(f"batch executor {name} must be an int >= {min_value}")
        return value

    @classmethod
    def _image_uri(cls, value: Any) -> Uri:
        if not isinstance(value, Uri):
            raise DmlRepoError("batch executor image must be a Uri")
        return value

    @classmethod
    def resolve_runnable(cls, uri, kwargs, sub):
        if sub is None:
            raise DmlRepoError("batch executor requires sub runnable")
        unknown = sorted(set(kwargs.keys()) - {"lambda_uri", "image", "cpu", "memory", "gpu"})
        if unknown:
            raise DmlRepoError(f"Unknown batch executor kwargs: {', '.join(unknown)}")
        return Runnable(
            target=Uri(cls._string("lambda_uri", kwargs.get("lambda_uri"))),
            adapter="dml-lambda-adapter",
            kwargs={
                "image": cls._image_uri(kwargs.get("image")),
                "cpu": cls._int("cpu", kwargs.get("cpu"), default=DEFAULT_VCPU, min_value=1),
                "memory": cls._int("memory", kwargs.get("memory"), default=DEFAULT_MEMORY, min_value=1),
                "gpu": cls._int("gpu", kwargs.get("gpu"), default=DEFAULT_GPU, min_value=0),
            },
            sub=sub,
        )

    @staticmethod
    def _client():
        return get_client("batch")

    @classmethod
    def _resource_requirements(cls, kwargs: dict[str, Any]) -> tuple[list[dict[str, str]], str]:
        cpu = cls._int("cpu", kwargs.get("cpu"), default=DEFAULT_VCPU, min_value=1)
        memory = cls._int("memory", kwargs.get("memory"), default=DEFAULT_MEMORY, min_value=1)
        gpu = cls._int("gpu", kwargs.get("gpu"), default=DEFAULT_GPU, min_value=0)
        reqs = [
            {"type": "MEMORY", "value": str(memory)},
            {"type": "VCPU", "value": str(cpu)},
        ]
        queue_env = "CPU_QUEUE"
        if gpu > 0:
            reqs.append({"type": "GPU", "value": str(gpu)})
            queue_env = "GPU_QUEUE"
        return reqs, cls._string(queue_env, os.environ.get(queue_env))

    def start(
        self,
        *,
        cache_key: str,
        execution_id: str,
        runnable: Runnable,
        argv_ptr: str,
        remote: dict[str, str],
    ) -> dict[str, Any]:
        if runnable is None or runnable.sub is None:
            raise DmlRepoError("batch executor start requires runnable with sub runnable")
        exec_state = ExecutionState(cache_key, remote_root=remote["root"])
        io = exec_state.adapter_io(execution_id, _ADAPTER_IO_NAME)
        payload = AdapterBase._dump_payload(
            runnable=runnable.sub,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
            execution_id=execution_id,
            remote=remote,
            state=None,
        )
        io.write_input(payload)
        client = self._client()
        reqs, job_queue = self._resource_requirements(runnable.kwargs)
        image = self._image_uri(runnable.kwargs.get("image"))
        job_name = f"dml-batch-{cache_key}"
        job_def = client.register_job_definition(
            jobDefinitionName=job_name,
            type="container",
            containerProperties={
                "image": image,
                "command": [runnable.sub.adapter, "--poll", "-i", io.input_uri, "-o", io.output_uri],
                "environment": [],
                "jobRoleArn": self._string("BATCH_TASK_ROLE_ARN", os.environ.get("BATCH_TASK_ROLE_ARN")),
                "resourceRequirements": reqs,
            },
        )["jobDefinitionArn"]
        job_id = client.submit_job(jobName=job_name, jobQueue=job_queue, jobDefinition=job_def)["jobId"]
        return {
            "status": "running",
            "error": None,
            "state": {
                "job_id": job_id,
                "job_definition": job_def,
            },
        }

    def poll(
        self,
        *,
        cache_key: str,
        execution_id: str,
        state: dict[str, Any],
        remote: dict[str, str],
    ) -> dict[str, Any]:
        job_id = state.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            return {"status": "failed", "error": "batch poll: missing job_id in job state"}
        try:
            jobs = self._client().describe_jobs(jobs=[job_id]).get("jobs", [])
        except Exception:
            return {"status": "running", "error": None, "state": state}
        if not jobs:
            return {"status": "running", "error": None, "state": state}
        job = jobs[0]
        job_status = job["status"]

        if job_status in PENDING_BATCH_STATUSES:
            return {"status": "running", "error": None, "state": state}

        if job_status == "SUCCEEDED":
            exec_state = ExecutionState(cache_key, remote_root=remote["root"])
            io = exec_state.adapter_io(execution_id, _ADAPTER_IO_NAME)
            try:
                raw = io.read_output()
                if raw is None:
                    return {"status": "failed", "error": "batch poll: sub-adapter output not yet written to S3"}
                result = json.loads(raw)
            except Exception as e:
                return {"status": "failed", "error": f"batch poll: could not read sub-adapter result: {e}"}
            if not isinstance(result, dict) or result.get("status") not in {"succeeded", "failed"}:
                return {"status": "failed", "error": f"batch poll: unexpected sub-adapter result: {result}"}
            return result

        # Failed
        reason = None
        if isinstance(job.get("statusReason"), str) and job["statusReason"]:
            reason = job["statusReason"]
        attempts = job.get("attempts") or [{}]
        container = attempts[-1].get("container", {}) if attempts else {}
        if isinstance(container, dict):
            reason = container.get("reason") or container.get("exitCode") or reason
        error = f"Batch job {job_id} failed"
        if reason not in {None, ""}:
            error = f"{error}: {reason}"
        return {"status": "failed", "error": error}
