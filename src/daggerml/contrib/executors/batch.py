from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any
from urllib.parse import urlparse

from botocore.exceptions import ClientError, ParamValidationError

from daggerml import Runnable, Uri
from daggerml._core import validate_adapter_response
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
_THROTTLING_CODES = {"Throttling", "ThrottlingException", "TooManyRequestsException", "RequestLimitExceeded"}


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
        code = (getattr(exc, "response", None) or {}).get("Error", {}).get("Code")
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
        if isinstance(value, bool) or not isinstance(value, int) or value < min_value:
            raise DmlRepoError(f"batch executor {name} must be an int >= {min_value}")
        return value

    @classmethod
    def _image_uri(cls, value: Any) -> Uri:
        if not isinstance(value, Uri):
            raise DmlRepoError("batch executor image must be a Uri")
        cls._string("image URI", value.uri)
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
        return self._launch(cache_key, execution_id, runnable, {}, remote, scratch_uri)

    def _launch(self, cache_key, execution_id, runnable, state, remote, scratch_uri) -> dict[str, Any]:
        state = dict(state)
        try:
            return self._start(cache_key, execution_id, runnable, state, remote, scratch_uri)
        except Exception as exc:
            if not self._is_throttling(exc):
                raise
            result = {"status": "retry", "error": None, "state": state}
            retry_after = self._retry_after(exc)
            if retry_after is not None:
                result["retry_after_ms"] = retry_after
            return result

    @staticmethod
    def _identity(execution_id, remote, scratch_uri):
        return hashlib.sha256(json.dumps([execution_id, remote["root"], scratch_uri]).encode()).hexdigest()

    def _save_submission(self, state, execution_id, remote, scratch_uri):
        _write_scratch_json(
            _scratch_uri(scratch_uri, "submission.json"),
            {"identity": self._identity(execution_id, remote, scratch_uri), "state": state},
            raw=False,
        )

    def _validate_submission(self, state, execution_id, remote, scratch_uri):
        if (state.get("phase") or "identity" in state) and (
            state.get("identity") != self._identity(execution_id, remote, scratch_uri)
        ):
            raise DmlRepoError("batch submission continuation belongs to another execution/remote")

    def _restore_submission(self, state, execution_id, remote, scratch_uri):
        self._validate_submission(state, execution_id, remote, scratch_uri)
        if state.get("job_id") or state.get("phase"):
            return dict(state)
        raw = _read_scratch_output(_scratch_uri(scratch_uri, "submission.json"))
        if raw is None:
            return dict(state)
        record = json.loads(raw)
        if record["identity"] != self._identity(execution_id, remote, scratch_uri):
            raise DmlRepoError("batch submission scratch belongs to another execution/remote")
        self._validate_submission(record["state"], execution_id, remote, scratch_uri)
        return record["state"]

    def _reconcile(self, state, execution_id, remote, scratch_uri):
        state = dict(state)
        if state["identity"] != self._identity(execution_id, remote, scratch_uri):
            raise DmlRepoError("batch submission intent belongs to another execution/remote")
        try:
            client = self._client(max_attempts=0)
            query = {
                "jobQueue": state["job_queue"],
                "filters": [{"name": "JOB_NAME", "values": [state["job_name"]]}],
                "maxResults": 100,
            }
            if state.get("next_token"):
                query["nextToken"] = state["next_token"]
            page = client.list_jobs(**query)
            candidates = [job["jobId"] for job in page.get("jobSummaryList", [])]
            jobs = client.describe_jobs(jobs=candidates).get("jobs", []) if candidates else []
            # An incomplete DescribeJobs response is not proof of absence. Replay
            # this page rather than advancing past jobs not yet visible.
            if {job["jobId"] for job in jobs} != set(candidates):
                return {"status": "retry", "error": None, "state": state, "retry_after_ms": 2000}
            matches = set(state.get("job_ids", []))
            for job in jobs:
                if (
                    job.get("jobName") == state["job_name"]
                    and job.get("jobQueue") == state["job_queue"]
                    and job.get("jobDefinition") == state["job_definition"]
                    and job.get("tags", {}).get("dml_identity") == state["identity"]
                    and job.get("createdAt", 0) >= state["submitted_after_ms"]
                ):
                    matches.add(job["jobId"])
            state["job_ids"] = sorted(matches)
            state["next_token"] = page.get("nextToken")
            if not state["next_token"] and matches:
                state["phase"] = "submitted" if len(matches) == 1 else "duplicates"
                if len(matches) == 1:
                    state["job_id"] = state["job_ids"][0]
            self._save_submission(state, execution_id, remote, scratch_uri)
        except Exception as exc:
            retry_after = self._retry_after(exc)
            return {
                "status": "retry", "error": None, "state": state,
                "retry_after_ms": 2000 if retry_after is None else retry_after,
            }
        return {"status": "retry", "error": None, "state": state, "retry_after_ms": 2000}

    @staticmethod
    def _confirmed_rejection(exc):
        if isinstance(exc, ParamValidationError):
            return True
        if not isinstance(exc, ClientError):
            return False
        code = exc.response.get("Error", {}).get("Code")
        return code in {
            "ClientException", "AccessDenied", "AccessDeniedException", "ValidationException",
            "UnrecognizedClientException", "InvalidClientTokenId", "SignatureDoesNotMatch",
        }

    def _start(self, cache_key, execution_id, runnable, state, remote, scratch_uri) -> dict[str, Any]:
        sub = runnable.get("sub")
        if sub is None:
            raise DmlRepoError("batch executor start requires runnable with sub runnable")
        kwargs = runnable.get("kwargs", {})
        reqs, job_queue = self._resource_requirements(kwargs)
        image = kwargs.get("image")
        # Runtime requests carry JSON URI objects, not authoring-time dataclasses.
        if isinstance(image, dict) and set(image) == {"uri"}:
            image = Uri(image["uri"])
        image = self._image_uri(image).uri
        adapter = self._string("sub adapter", sub.get("adapter"))
        task_role = self._string("BATCH_TASK_ROLE_ARN", os.environ.get("BATCH_TASK_ROLE_ARN"))
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
        restored = self._restore_submission(state, execution_id, remote, scratch_uri)
        state.update(restored)
        if state.get("phase") == "unknown":
            return self._reconcile(state, execution_id, remote, scratch_uri)
        if state.get("job_id") or state.get("phase") in {"duplicates", "rejected"}:
            return self.poll(cache_key, execution_id, runnable, state, remote, scratch_uri)
        _write_scratch_json(input_uri, payload, raw=True)
        client = self._client(max_attempts=_BATCH_START_MAX_ATTEMPTS)
        region = self._string("AWS region", client.meta.region_name)
        identity = self._identity(execution_id, remote, scratch_uri)
        job_name = f"dml-batch-{identity}"
        job_def = state.get("job_definition") or client.register_job_definition(
            jobDefinitionName=job_name,
            type="container",
            containerProperties={
                "image": image,
                "command": [adapter, "--poll", "-i", input_uri, "-o", output_uri],
                "environment": [
                    {"name": "AWS_EC2_METADATA_DISABLED", "value": "true"},
                    {"name": "AWS_REGION", "value": region},
                    {"name": "AWS_DEFAULT_REGION", "value": region},
                ],
                "privileged": False,
                "jobRoleArn": task_role,
                "resourceRequirements": reqs,
            },
        )["jobDefinitionArn"]
        state["job_definition"] = job_def
        state["identity"] = identity
        try:
            queues = client.describe_job_queues(jobQueues=[job_queue]).get("jobQueues", [])
            if not queues:
                raise DmlRepoError(f"batch queue not found: {job_queue}")
            job_queue = queues[0]["jobQueueArn"]
        except Exception as exc:
            if self._is_throttling(exc):
                raise
            return self._rollback(client, job_def, exc, state)
        intent = {
            **state, "phase": "unknown", "identity": identity,
            "execution_id": execution_id, "remote_root": remote["root"], "scratch_uri": scratch_uri,
            "job_queue": job_queue, "job_name": job_name,
            "submitted_after_ms": int(time.time() * 1000) - 60000,
        }
        # Persist BEFORE SubmitJob. A crash here can leave an intent with no job;
        # absence is never enough evidence to safely submit again.
        try:
            self._save_submission(intent, execution_id, remote, scratch_uri)
        except Exception as exc:
            if self._is_throttling(exc):
                raise
            # SubmitJob has not been called, even if S3 stored the intent before
            # losing its response. Retain a rejection receipt for safe teardown.
            rejected = {**state, "phase": "rejected", "identity": identity}
            return self._rollback(client, job_def, exc, rejected)
        try:
            # SubmitJob has no idempotency token. Disable ALL SDK retries so a
            # final rejection cannot conceal an earlier accepted SDK attempt.
            job_id = self._client(max_attempts=0).submit_job(
                jobName=job_name, jobQueue=job_queue, jobDefinition=job_def,
                tags={"dml_identity": identity},
            )["jobId"]
        except Exception as exc:
            if self._is_throttling(exc) or self._confirmed_rejection(exc):
                try:
                    self._save_submission(state, execution_id, remote, scratch_uri)
                except Exception as save_exc:
                    rejected = {**state, "phase": "rejected", "identity": identity}
                    error = DmlRepoError(f"{exc}; rejection persistence failed: {save_exc}")
                    return self._rollback(client, job_def, error, rejected)
                if self._is_throttling(exc):
                    raise
                return self._rollback(client, job_def, exc, state)
            return {"status": "retry", "error": None, "state": intent, "retry_after_ms": 2000}
        accepted = {**intent, "phase": "submitted", "job_id": job_id}
        try:
            self._save_submission(accepted, execution_id, remote, scratch_uri)
        except Exception:
            # The intent remains recoverable even if this continuation is lost.
            pass
        return {"status": "retry", "error": None, "state": accepted}

    @staticmethod
    def _rollback(client, job_def, exc, state):
        try:
            client.deregister_job_definition(jobDefinition=job_def)
        except Exception as cleanup_exc:
            return {
                "status": "failure",
                "error": f"batch submission failed: {exc}; rollback failed for {job_def}: {cleanup_exc}",
                "state": state,
            }
        return {"status": "failure", "error": f"batch submission failed: {exc}", "state": state}

    def poll(
        self,
        cache_key: str,
        execution_id: str,
        runnable: dict[str, Any],
        state: dict[str, Any],
        remote: dict[str, str],
        scratch_uri: str,
    ) -> dict[str, Any]:
        self._validate_submission(state, execution_id, remote, scratch_uri)
        if state.get("phase") == "rejected":
            return {"status": "failure", "error": "batch submission rejected; cleanup required", "state": state}
        if state.get("phase") == "unknown":
            return self._reconcile(state, execution_id, remote, scratch_uri)
        if state.get("phase") == "duplicates":
            return {
                "status": "failure", "error": f"batch submission has multiple matching jobs: {state['job_ids']}",
                "state": state,
            }
        if not state or (state.get("job_definition") and not state.get("job_id")):
            return self._launch(cache_key, execution_id, runnable, state, remote, scratch_uri)
        del cache_key, execution_id, runnable, remote
        job_id = state.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            return {
                "status": "failure",
                "error": "batch poll: missing job_id in job state",
                "state": state,
            }
        try:
            jobs = self._client().describe_jobs(jobs=[job_id]).get("jobs", [])
        except Exception as exc:
            if self._is_throttling(exc):
                result = {"status": "retry", "error": None, "state": state}
                retry_after = self._retry_after(exc)
                if retry_after is not None:
                    result["retry_after_ms"] = retry_after
                return result
            return {"status": "failure", "error": f"batch status check failed: {exc}", "state": state}
        if not jobs:
            return {"status": "retry", "error": None, "state": state}
        job = jobs[0]
        job_status = job["status"]

        if job_status in PENDING_BATCH_STATUSES:
            return {"status": "retry", "error": None, "state": state}

        if job_status == "SUCCEEDED":
            try:
                raw = _read_scratch_output(_scratch_uri(scratch_uri, "output.json"))
                if raw is None:
                    return {
                        "status": "failure",
                        "error": "batch poll: sub-adapter output not yet written to S3",
                        "state": state,
                    }
                result = json.loads(raw)
            except Exception as e:
                return {
                    "status": "failure",
                    "error": f"batch poll: could not read sub-adapter result: {e}",
                    "state": state,
                }
            try:
                result = validate_adapter_response(result)
            except DmlRepoError as exc:
                raise DmlRepoError(f"batch poll: invalid nested adapter output: {result}") from exc
            nested_state = result.pop("adapter_state", None)
            next_state = {**state, "nested_adapter_state": nested_state} if isinstance(nested_state, dict) else state
            return {**result, "state": next_state}

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
        return {"status": "failure", "error": error, "state": state}

    @staticmethod
    def _is_throttling(exc: Exception) -> bool:
        return (getattr(exc, "response", None) or {}).get("Error", {}).get("Code") in _THROTTLING_CODES

    @staticmethod
    def _retry_after(exc: Exception) -> int | None:
        headers = (getattr(exc, "response", None) or {}).get("ResponseMetadata", {}).get("HTTPHeaders", {})
        value = headers.get("retry-after")
        try:
            return max(0, int(float(value) * 1000))
        except (TypeError, ValueError):
            return None

    def cleanup(self, cache_key, execution_id, runnable, state, remote, scratch_uri, result_ref):
        return self._teardown(state, execution_id, remote, scratch_uri, cancel=False)

    def cancel(
        self,
        cache_key: str,
        execution_id: str,
        runnable: dict[str, Any],
        state: dict[str, Any],
        remote: dict[str, str],
        scratch_uri: str,
        cancel_requested_by: str | None,
        argv_ref: str | None = None,
    ) -> dict[str, Any]:
        return self._teardown(state, execution_id, remote, scratch_uri, cancel=True)

    def _teardown(self, state, execution_id, remote, scratch_uri, *, cancel):
        state = dict(state) if isinstance(state, dict) else {}
        try:
            state = self._restore_submission(state, execution_id, remote, scratch_uri)
            if state.get("phase") == "unknown":
                return self._reconcile(state, execution_id, remote, scratch_uri)
            client = self._client()
            ids = state.get("job_ids") or ([state["job_id"]] if state.get("job_id") else [])
            offset = state.get("teardown_offset", 0)
            current = ids[offset:offset + 100]
            if current:
                if cancel:
                    for job_id in current:
                        client.terminate_job(jobId=job_id, reason="daggerml cancellation requested")
                jobs = client.describe_jobs(jobs=current).get("jobs", [])
                if len(jobs) != len(current) or any(job.get("status") not in {"SUCCEEDED", "FAILED"} for job in jobs):
                    return {"status": "retry", "error": None, "state": state}
                if offset + 100 < len(ids):
                    state["teardown_offset"] = offset + 100
                    return {"status": "retry", "error": None, "state": state}
            if state.get("job_definition"):
                client.deregister_job_definition(jobDefinition=state["job_definition"])
        except Exception as exc:
            if self._is_throttling(exc):
                result = {"status": "retry", "error": None, "state": state}
                retry_after = self._retry_after(exc)
                if retry_after is not None:
                    result["retry_after_ms"] = retry_after
                return result
            return {"status": "failure", "error": f"batch teardown failed: {exc}", "state": state}
        return {"status": "cancelled" if cancel else "success", "error": None, "state": state}
