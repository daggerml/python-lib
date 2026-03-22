from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from daggerml import Uri
from daggerml._internal.types import DmlRepoError, Runnable
from daggerml.contrib.adapters import AdapterBase
from daggerml.contrib.executor_state import is_stale
from daggerml.contrib.executors._lambda import LambdaExecutorBase
from daggerml.contrib.s3 import S3Store
from daggerml.util import get_client

PENDING_BATCH_STATUSES = {"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"}
DEFAULT_VCPU = 1
DEFAULT_MEMORY = 16 * 1024
DEFAULT_GPU = 0


@dataclass
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

    @staticmethod
    def _store(remote: dict[str, str]) -> S3Store:
        return S3Store.from_remote_root(remote["root"]).cd("jobs").cd(remote["cache"])

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

    @classmethod
    def start(cls, *, runnable, argv_ptr, cache_key, remote, state):
        if runnable is None or runnable.sub is None:
            raise DmlRepoError("batch executor start requires runnable with sub runnable")
        store = cls._store(remote)
        input_uri = store.put(
            data=AdapterBase._dump_payload(
                runnable=runnable.sub,
                argv_ptr=argv_ptr,
                cache_key=cache_key,
                remote=remote,
                comms=cls._child_comms(state),
            ),
            suffix=".json",
        ).uri
        client = cls._client()
        reqs, job_queue = cls._resource_requirements(runnable.kwargs)
        image = cls._image_uri(runnable.kwargs.get("image"))
        job_name = f"dml-batch-{cache_key}"
        job_def = client.register_job_definition(
            jobDefinitionName=job_name,
            type="container",
            containerProperties={
                "image": image,
                "command": [runnable.sub.adapter, "--poll", "-i", input_uri],
                "environment": [
                    {"name": "DML_REMOTE_ROOT", "value": remote["root"]},
                    {"name": "DML_REMOTE_CACHE", "value": remote["cache"]},
                ],
                "jobRoleArn": cls._string("BATCH_TASK_ROLE_ARN", os.environ.get("BATCH_TASK_ROLE_ARN")),
                "resourceRequirements": reqs,
            },
        )["jobDefinitionArn"]
        job_id = client.submit_job(jobName=job_name, jobQueue=job_queue, jobDefinition=job_def)["jobId"]
        state.put_if_absent(state.init_record(status="pending"))
        state.update(
            state.set_executor_metadata(
                cls.name,
                data={"job_id": job_id, "job_definition": job_def, "input_uri": input_uri},
            )
        )
        return {"status": "pending", "error": None}

    @classmethod
    def poll(cls, state):
        metadata = state.get_executor_metadata(cls.name)
        job_id = cls._string("job_id", metadata.get("job_id"))
        try:
            jobs = cls._client().describe_jobs(jobs=[job_id]).get("jobs", [])
        except Exception:
            return {"status": metadata.get("batch_status", "pending"), "error": None}
        if not jobs:
            return {"status": metadata.get("batch_status", "pending"), "error": None}
        job = jobs[0]
        job_status = job["status"]
        if job_status in PENDING_BATCH_STATUSES:
            batch_status = "running" if job_status == "RUNNING" else "pending"
            if is_stale(state.get()):
                batch_status = "failed"
            metadata["batch_status"] = batch_status
            state.update(state.set_executor_metadata(cls.name, data=metadata))
            return {"status": batch_status, "error": None}
        if job_status == "SUCCEEDED":
            metadata["batch_status"] = "succeeded"
            state.update(state.set_executor_metadata(cls.name, data=metadata))
            return {"status": "succeeded", "error": None}
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
        metadata["batch_status"] = "failed"
        state.update(state.set_executor_metadata(cls.name, data=metadata))
        return {"status": "failed", "error": error}

    @classmethod
    def gc(cls, *, state=None):
        if state is None:
            raise DmlRepoError("batch gc requires locked state")
        metadata = state.get_executor_metadata(cls.name)
        job_id = metadata.get("job_id")
        if isinstance(job_id, str) and job_id:
            try:
                cls._client().terminate_job(jobId=job_id, reason="killed")
            except Exception:
                try:
                    cls._client().cancel_job(jobId=job_id, reason="killed")
                except Exception:
                    pass
        job_def = metadata.get("job_definition")
        if isinstance(job_def, str) and job_def:
            try:
                cls._client().deregister_job_definition(jobDefinition=job_def)
            except Exception:
                pass
        state.delete()
