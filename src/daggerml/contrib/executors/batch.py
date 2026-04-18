from __future__ import annotations

import os
from typing import Any

from daggerml import Uri
from daggerml._internal.types import DmlRepoError, Runnable
from daggerml.contrib.adapters import AdapterBase
from daggerml.contrib.executor_state import ExecutionRecord, ExecutionState
from daggerml.contrib.executors._lambda import LambdaExecutorBase
from daggerml.contrib.s3 import S3Store
from daggerml.util import get_client

PENDING_BATCH_STATUSES = {"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"}
HEARTBEAT_STALENESS = 60.0
DEFAULT_VCPU = 1
DEFAULT_MEMORY = 16 * 1024
DEFAULT_GPU = 0


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
        store = S3Store.from_remote_root(remote["root"])
        return store.cd("jobs")

    @staticmethod
    def _child_cache_key(cache_key: str) -> str:
        return f"{cache_key}:batch-child"

    @staticmethod
    def _terminal_child_state(child_cache_key: str) -> ExecutionRecord:
        child = ExecutionState(child_cache_key).get()
        if child is None:
            raise DmlRepoError(f"Batch nested execution missing child state for cache_key={child_cache_key!r}")
        if child["status"] not in {"succeeded", "failed"}:
            raise DmlRepoError(
                f"Batch nested execution reached terminal Batch status but child state is {child['status']!r}"
            )
        return child

    @staticmethod
    def _project_child_terminal(*, cache_key: str, child_cache_key: str) -> None:
        parent = ExecutionState(cache_key)
        child = BatchExecutor._terminal_child_state(child_cache_key)
        if not parent.lock():
            return
        try:
            if child["status"] == "succeeded":
                dag_id = child.get("dag_id")
                if not isinstance(dag_id, str) or not dag_id:
                    raise DmlRepoError("Batch nested execution succeeded without dag_id")
                parent.mark_succeeded(dag_id)
                return
            error = child.get("error")
            if not isinstance(error, str) or not error:
                error = "Batch nested execution failed without error"
            parent.mark_failed(error)
        finally:
            parent.unlock()

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
        self, *, cache_key: str, state: ExecutionRecord, runnable: Runnable, argv_ptr: str, remote: dict[str, str]
    ) -> None:
        if runnable is None or runnable.sub is None:
            raise DmlRepoError("batch executor start requires runnable with sub runnable")
        child_cache_key = self._child_cache_key(cache_key)
        ExecutionState.upsert(child_cache_key, argv_ptr)
        store = self._store(remote)
        input_uri = store.put(
            data=AdapterBase._dump_payload(
                runnable=runnable.sub,
                argv_ptr=argv_ptr,
                cache_key=child_cache_key,
                remote=remote,
            ),
            suffix=".json",
        ).uri
        client = self._client()
        reqs, job_queue = self._resource_requirements(runnable.kwargs)
        image = self._image_uri(runnable.kwargs.get("image"))
        job_name = f"dml-batch-{cache_key}"

        docker_env = [
            {"name": "DML_REMOTE_ROOT", "value": remote["root"]},
            {"name": "DML_DYNAMODB_TABLE", "value": os.environ.get("DML_DYNAMODB_TABLE", "")},
        ]

        job_def = client.register_job_definition(
            jobDefinitionName=job_name,
            type="container",
            containerProperties={
                "image": image,
                "command": [runnable.sub.adapter, "--poll", "-i", input_uri],
                "environment": docker_env,
                "jobRoleArn": self._string("BATCH_TASK_ROLE_ARN", os.environ.get("BATCH_TASK_ROLE_ARN")),
                "resourceRequirements": reqs,
            },
        )["jobDefinitionArn"]
        job_id = client.submit_job(jobName=job_name, jobQueue=job_queue, jobDefinition=job_def)["jobId"]

        es = ExecutionState(cache_key)
        assert es.lock()
        try:
            es.update_metadata(
                {
                    self.name: {
                        "child_cache_key": child_cache_key,
                        "job_id": job_id,
                        "job_definition": job_def,
                        "input_uri": input_uri,
                    },
                }
            )
        finally:
            es.unlock()

    def poll(self, *, cache_key: str, state: ExecutionRecord) -> None:
        import time

        meta = (state.get("metadata") or {}).get(self.name, {})
        child_cache_key = meta.get("child_cache_key")
        job_id = meta.get("job_id")
        if not isinstance(child_cache_key, str) or not child_cache_key or not isinstance(job_id, str) or not job_id:
            return
        try:
            jobs = self._client().describe_jobs(jobs=[job_id]).get("jobs", [])
        except Exception:
            return
        if not jobs:
            return
        job = jobs[0]
        job_status = job["status"]

        es = ExecutionState(cache_key)

        if job_status in PENDING_BATCH_STATUSES:
            # Check heartbeat staleness
            if state["heartbeat_ts"] is not None and state["heartbeat_ts"] + HEARTBEAT_STALENESS < time.time():
                if es.lock():
                    try:
                        es.mark_failed(f"Batch job {job_id} heartbeat stale")
                    finally:
                        es.unlock()
            return

        if job_status == "SUCCEEDED":
            try:
                self._project_child_terminal(cache_key=cache_key, child_cache_key=child_cache_key)
            except DmlRepoError as e:
                if es.lock():
                    try:
                        es.mark_failed(str(e))
                    finally:
                        es.unlock()
            return

        # Failed
        reason = None
        if isinstance(job.get("statusReason"), str) and job["statusReason"]:
            reason = job["statusReason"]
        attempts = job.get("attempts") or [{}]
        container = attempts[-1].get("container", {}) if attempts else {}
        if isinstance(container, dict):
            reason = container.get("reason") or container.get("exitCode") or reason
        try:
            self._project_child_terminal(cache_key=cache_key, child_cache_key=child_cache_key)
            return
        except DmlRepoError:
            pass
        error = f"Batch job {job_id} failed"
        if reason not in {None, ""}:
            error = f"{error}: {reason}"
        if es.lock():
            try:
                es.mark_failed(error)
            finally:
                es.unlock()

    def cleanup(self, *, cache_key: str, state: ExecutionRecord) -> None:
        meta = (state.get("metadata") or {}).get(self.name, {})
        job_id = meta.get("job_id")
        if isinstance(job_id, str) and job_id:
            try:
                self._client().terminate_job(jobId=job_id, reason="killed")
            except Exception:
                try:
                    self._client().cancel_job(jobId=job_id, reason="killed")
                except Exception:
                    pass
        job_def = meta.get("job_definition")
        if isinstance(job_def, str) and job_def:
            try:
                self._client().deregister_job_definition(jobDefinition=job_def)
            except Exception:
                pass
