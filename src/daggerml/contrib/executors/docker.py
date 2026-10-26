from __future__ import annotations

import fcntl
import json
import os
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, cast
from urllib.parse import urlparse

from daggerml import Runnable, Uri
from daggerml._core import validate_adapter_response
from daggerml.api import DmlRepoError
from daggerml.contrib.executors._base import ExecutorBase
from daggerml.contrib.executors._ownership import _current_owner
from daggerml.contrib.s3 import S3Store, is_s3_uri
from daggerml.util import get_client

_CONTAINER_LABEL = "daggerml.managed=true"
_IMAGE_LOCK_PATH = Path(os.getenv("XDG_CACHE_HOME") or Path.home() / ".cache") / "daggerml" / "docker-images.lock"


@contextmanager
def _image_lock() -> Iterator[None]:
    """Coordinate cooperating local adapter processes, including fresh callers."""
    _IMAGE_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _IMAGE_LOCK_PATH.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _inspect_image(image: str, docker_bin: str | None = None) -> dict[str, Any] | None:
    try:
        output = DockerExecutor._run_docker("image", "inspect", image, docker_bin=docker_bin)
    except DmlRepoError as exc:
        if "No such image:" in str(exc) or "No such object:" in str(exc):
            return None
        raise
    inspection = json.loads(output)
    if (not isinstance(inspection, list) or len(inspection) != 1
            or not isinstance(inspection[0], dict) or not isinstance(inspection[0].get("Id"), str)):
        raise DmlRepoError("docker image inspect returned invalid image metadata")
    return inspection[0]


def _scratch_uri(scratch_uri: str, filename: str) -> str:
    parsed = urlparse(scratch_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise DmlRepoError("Execution scratch URI must be an s3:// URI")
    prefix = parsed.path.lstrip("/").rstrip("/")
    return f"s3://{parsed.netloc}/{prefix}/local:docker/{filename}"


def _write_scratch_json(uri: str, payload: Any, *, raw: bool) -> None:
    parsed = urlparse(uri)
    data = payload if raw else json.dumps(payload)
    get_client("s3").put_object(
        Bucket=parsed.netloc,
        Key=parsed.path.lstrip("/"),
        Body=data.encode("utf-8"),
        ContentType="application/json",
    )


def _read_scratch_output(uri: str) -> str | None:
    parsed = urlparse(uri)
    try:
        response = get_client("s3").get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    return response["Body"].read().decode("utf-8")


class DockerExecutor(ExecutorBase):
    name = "docker"
    adapter = "local"

    @classmethod
    def resolve_runnable(cls, uri, kwargs, sub):
        if sub is None:
            raise DmlRepoError("docker executor requires sub runnable")
        image = kwargs.get("image")
        if image is None:
            raise DmlRepoError("docker executor requires image")
        unknown = sorted(set(kwargs.keys()) - {"image", "flags"})
        if unknown:
            raise DmlRepoError(f"Unknown docker executor kwargs: {', '.join(unknown)}")
        return Runnable(
            target=Uri("docker"),
            kwargs={"image": image, "flags": kwargs.get("flags", [])},
            sub=sub,
            adapter="dml-local-adapter",
        )

    @staticmethod
    def _run_docker(*args: str, check: bool = True, docker_bin: str | None = None) -> str:
        docker_bin = docker_bin or shutil.which("docker")
        if docker_bin is None:
            raise DmlRepoError("docker executable not found in PATH")
        proc = subprocess.run([docker_bin, *args], check=False, capture_output=True, text=True)
        if proc.returncode == 0:
            return proc.stdout.strip() or proc.stderr.strip()
        if check:
            command = f"{docker_bin} {' '.join(args)}"
            raise DmlRepoError(
                f"docker command failed ({proc.returncode}): {command}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
            )
        return proc.stdout.strip() or proc.stderr.strip()

    @staticmethod
    def _image_input(runnable: dict[str, Any]) -> str:
        image = runnable.get("kwargs", {}).get("image")
        if isinstance(image, dict):
            image = image.get("uri")
        if isinstance(image, Uri):
            return image.uri
        if isinstance(image, str) and image:
            return image
        raise DmlRepoError("docker executor image must resolve to a non-empty Uri or string")

    @staticmethod
    def _stage_image(runnable: dict[str, Any], workdir: Path, remote: dict[str, Any]) -> str | Path:
        """Download archives before acquiring the daemon lifecycle lock."""
        image = DockerExecutor._image_input(runnable)
        if not is_s3_uri(image):
            return image
        tar_path = workdir / "image.tar"
        store = S3Store.from_remote_root(cast(str, remote["root"]))
        tar_path.write_bytes(store.get(image))
        return tar_path

    @staticmethod
    def _prepare_image(image: str | Path) -> str:
        """Resolve Docker's loaded image ID; caller holds the lock through launch."""
        if isinstance(image, str):
            return image
        output = DockerExecutor._run_docker("load", "-i", str(image))
        references = set()
        for line in output.splitlines():
            for prefix in ("Loaded image: ", "Loaded image ID: "):
                if line.strip().startswith(prefix):
                    reference = line.strip().removeprefix(prefix).strip()
                    if reference:
                        references.add(reference)
        if not references:
            raise DmlRepoError("docker load did not report any image references")
        image_ids = set()
        for reference in sorted(references):
            inspection = _inspect_image(reference)
            if inspection is None:
                raise DmlRepoError(f"docker load reported an unavailable image: {reference}")
            image_ids.add(inspection["Id"])
        if len(image_ids) != 1:
            raise DmlRepoError("docker load references must resolve to exactly one image ID")
        return image_ids.pop()

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
            raise DmlRepoError("docker executor requires sub runnable")
        input_uri = _scratch_uri(scratch_uri, "input.json")
        output_uri = _scratch_uri(scratch_uri, "output.json")

        workdir = Path(tempfile.mkdtemp(prefix=f"dml-docker-{execution_id}-"))
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
        try:
            _write_scratch_json(input_uri, payload, raw=True)
            staged_image = self._stage_image(runnable, workdir, remote)
            with _image_lock():
                image_ref = self._prepare_image(staged_image)
                container_id = self._run_docker(
                    "run",
                    "-d",
                    *cast(list[str], runnable.get("kwargs", {}).get("flags", [])),
                    "--label",
                    _CONTAINER_LABEL,
                    "-e",
                    f"DML_REMOTE_ROOT={remote['root']}",
                    image_ref,
                    sub["adapter"],
                    "--poll",
                    "-i",
                    input_uri,
                    "-o",
                    output_uri,
                )
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

        return {
            "status": "retry",
            "error": None,
            "state": {
                "owner": _current_owner(),
                "container_id": container_id,
                "image": image_ref,
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
        if state["owner"] != _current_owner():
            return {"status": "retry", "error": None, "state": state}
        container_id = state.get("container_id")

        if not isinstance(container_id, str) or not container_id:
            return {
                "status": "failure",
                "error": "docker poll: missing container_id in job state",
                "state": state,
            }

        docker_bin = shutil.which("docker")
        if docker_bin is None:
            return {
                "status": "failure",
                "error": "docker poll: docker executable not found",
                "state": state,
            }

        proc = subprocess.run(
            [docker_bin, "inspect", "--format", "{{.State.Status}}", container_id],
            check=False,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            # An inspect error cannot establish that the container has exited.
            return {"status": "retry", "error": None, "state": state}
        else:
            container_status = proc.stdout.strip()

        if container_status in ("created", "running", "paused", "restarting"):
            return {"status": "retry", "error": None, "state": state}

        raw = _read_scratch_output(_scratch_uri(scratch_uri, "output.json"))
        if raw is not None:
            try:
                result = validate_adapter_response(json.loads(raw))
                result.pop("adapter_state", None)
                return {**result, "state": state}
            except Exception as e:
                raise DmlRepoError(f"docker poll: invalid nested adapter output: {e}") from e

        return {
            "status": "failure",
            "error": f"docker container {container_id} exited without output",
            "state": state,
        }

    def cleanup(self, cache_key, execution_id, runnable, state, remote, scratch_uri, result_ref):
        del cache_key, execution_id, runnable, remote, scratch_uri, result_ref
        if isinstance(state, dict) and state["owner"] != _current_owner():
            return {"status": "retry", "error": None, "state": state}
        state = state if isinstance(state, dict) else {}
        container_id = state.get("container_id")
        docker_bin = shutil.which("docker")
        if not isinstance(container_id, str) or not container_id or docker_bin is None:
            return {"status": "success", "error": None, "state": state}
        status = self._run_docker(
            "inspect", "--format", "{{.State.Status}}", container_id, check=False, docker_bin=docker_bin
        )
        if status in {"created", "running", "paused", "restarting"}:
            return {"status": "retry", "error": None, "state": state}
        return {"status": "success", "error": None, "state": state}

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
        del cache_key, execution_id, runnable, remote, scratch_uri, cancel_requested_by, argv_ref
        if isinstance(state, dict) and state["owner"] != _current_owner():
            return {"status": "retry", "error": None, "state": state}
        state = state if isinstance(state, dict) else {}
        docker_bin = shutil.which("docker")
        container_id = state.get("container_id")
        if docker_bin is None:
            if isinstance(container_id, str) and container_id:
                return {"status": "failure", "error": "docker executable not found in PATH", "state": state}
            return {"status": "cancelled", "error": None, "state": state}
        if isinstance(container_id, str) and container_id:
            try:
                self._run_docker("update", "--restart=no", container_id, docker_bin=docker_bin)
                self._run_docker("stop", container_id, docker_bin=docker_bin)
            except DmlRepoError as exc:
                if "No such container:" not in str(exc):
                    raise
        return {"status": "cancelled", "error": None, "state": state}
