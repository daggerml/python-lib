from __future__ import annotations

import json
import os
from typing import Any
from urllib.parse import urlparse

from daggerml import Runnable, Uri
from daggerml.api import DmlRepoError
from daggerml.contrib.executors.lambda_ import LambdaExecutorBase
from daggerml.util import get_client

PENDING_BATCH_STATUSES = {"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"}
DEFAULT_VCPU = 1
DEFAULT_MEMORY = 16 * 1024
DEFAULT_GPU = 0
_BATCH_CONNECTION_TIMEOUT = 60
_BATCH_READ_TIMEOUT = 60
_BATCH_START_MAX_ATTEMPTS = 100
_BATCH_POLL_MAX_ATTEMPTS = 25
_BATCH_MAX_POOL_CONNECTIONS = 100

_ADAPTER_IO_NAME = "lambda:batch"


def _batch_client(name: str, *, max_attempts: int = _BATCH_POLL_MAX_ATTEMPTS):
    return get_client(
        name,
        connection_timeout=_BATCH_CONNECTION_TIMEOUT,
        read_timeout=_BATCH_READ_TIMEOUT,
        max_attempts=max_attempts,
        retry_mode="adaptive",
        max_pool_connections=_BATCH_MAX_POOL_CONNECTIONS,
    )


def _scratch_uri(scratch_uri: str, filename: str) -> str:
    parsed = urlparse(scratch_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise DmlRepoError("Execution scratch URI must be an s3:// URI")
    prefix = parsed.path.lstrip("/").rstrip("/")
    return f"s3://{parsed.netloc}/{prefix}/{_ADAPTER_IO_NAME}/{filename}"


def _write_scratch_json(uri: str, payload: Any, *, raw: bool) -> None:
    parsed = urlparse(uri)
    data = payload if raw else json.dumps(payload)
    _batch_client("s3", max_attempts=_BATCH_START_MAX_ATTEMPTS).put_object(
        Bucket=parsed.netloc,
        Key=parsed.path.lstrip("/"),
        Body=data.encode("utf-8"),
        ContentType="application/json",
    )


def _read_scratch_output(uri: str) -> str | None:
    parsed = urlparse(uri)
    try:
        response = _batch_client("s3").get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    return response["Body"].read().decode("utf-8")


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
    def _client(*, max_attempts: int = _BATCH_POLL_MAX_ATTEMPTS):
        return _batch_client("batch", max_attempts=max_attempts)

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
        runnable: dict[str, Any],
        remote: dict[str, str],
        scratch_uri: str,
    ) -> dict[str, Any]:
        sub = runnable.get("sub")
        if sub is None:
            raise DmlRepoError("batch executor start requires runnable with sub runnable")
        input_uri = _scratch_uri(scratch_uri, "input.json")
        output_uri = _scratch_uri(scratch_uri, "output.json")
        payload = json.dumps(
            {
                "operation": "invoke",
                "runnable": sub,
                "cache_key": cache_key,
                "execution_id": execution_id,
                "remote": remote,
                "scratch_uri": scratch_uri,
                "adapter_state": None,
            }
        )
        _write_scratch_json(input_uri, payload, raw=True)
        client = self._client(max_attempts=_BATCH_START_MAX_ATTEMPTS)
        kwargs = runnable.get("kwargs", {})
        reqs, job_queue = self._resource_requirements(kwargs)
        image = self._image_uri(kwargs.get("image"))
        job_name = f"dml-batch-{cache_key}"
        job_def = client.register_job_definition(
            jobDefinitionName=job_name,
            type="container",
            containerProperties={
                "image": image,
                "command": [sub["adapter"], "--poll", "-i", input_uri, "-o", output_uri],
                "environment": [],
                "jobRoleArn": self._string("BATCH_TASK_ROLE_ARN", os.environ.get("BATCH_TASK_ROLE_ARN")),
                "resourceRequirements": reqs,
            },
        )["jobDefinitionArn"]
        job_id = client.submit_job(jobName=job_name, jobQueue=job_queue, jobDefinition=job_def)["jobId"]
        return {
            "status": "running",
            "error": None,
            "dag_id": None,
            "state": {
                "job_id": job_id,
                "job_definition": job_def,
            },
        }

    def poll(
        self,
        cache_key: str,
        execution_id: str,
        runnable: dict[str, Any],
        state: dict[str, Any],
        remote: dict[str, str],
        scratch_uri: str,
    ) -> dict[str, Any]:
        del cache_key, execution_id, runnable, remote
        job_id = state.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            return {
                "status": "failed",
                "error": "batch poll: missing job_id in job state",
                "state": state,
                "dag_id": None,
            }
        try:
            jobs = self._client().describe_jobs(jobs=[job_id]).get("jobs", [])
        except Exception:
            return {"status": "running", "error": None, "state": state, "dag_id": None}
        if not jobs:
            return {"status": "running", "error": None, "state": state, "dag_id": None}
        job = jobs[0]
        job_status = job["status"]

        if job_status in PENDING_BATCH_STATUSES:
            return {"status": "running", "error": None, "state": state, "dag_id": None}

        if job_status == "SUCCEEDED":
            try:
                raw = _read_scratch_output(_scratch_uri(scratch_uri, "output.json"))
                if raw is None:
                    return {
                        "status": "failed",
                        "error": "batch poll: sub-adapter output not yet written to S3",
                        "state": state,
                        "dag_id": None,
                    }
                result = json.loads(raw)
            except Exception as e:
                return {
                    "status": "failed",
                    "error": f"batch poll: could not read sub-adapter result: {e}",
                    "state": state,
                    "dag_id": None,
                }
            if result.get("status") not in {"succeeded", "failed"}:
                return {
                    "status": "failed",
                    "error": f"batch poll: unexpected sub-adapter result: {result}",
                    "state": state,
                    "dag_id": None,
                }
            nested_state = result.pop("adapter_state", None)
            if not isinstance(nested_state, dict):
                return {
                    "status": "failed",
                    "error": "batch poll: sub-adapter result missing object adapter_state",
                    "state": state,
                    "dag_id": None,
                }
            return {**result, "state": {**state, "nested_adapter_state": nested_state}}

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
        return {"status": "failed", "error": error, "state": state, "dag_id": None}

    def cancel(
        self,
        cache_key: str,
        execution_id: str,
        runnable: dict[str, Any],
        state: dict[str, Any],
        remote: dict[str, str],
        scratch_uri: str,
        cancel_requested_by: str | None,
        argv_ptr: str | None = None,
    ) -> dict[str, Any]:
        del cache_key, execution_id, runnable, remote, scratch_uri, cancel_requested_by, argv_ptr
        client = self._client()
        job_id = state.get("job_id")
        job_definition = state.get("job_definition")
        if isinstance(job_id, str) and job_id:
            try:
                client.cancel_job(jobId=job_id, reason="daggerml cancellation requested")
            except Exception:
                try:
                    client.terminate_job(jobId=job_id, reason="daggerml cancellation requested")
                except Exception:
                    pass
        if isinstance(job_definition, str) and job_definition:
            try:
                client.deregister_job_definition(jobDefinition=job_definition)
            except Exception:
                pass
        return {"status": "cancelled", "error": None, "state": state}
