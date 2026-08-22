from __future__ import annotations

import json
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse

from daggerml import Runnable, Uri
from daggerml.api import DmlRepoError
from daggerml.contrib.executors._base import ExecutorBase
from daggerml.contrib.s3 import S3Store, is_s3_uri
from daggerml.util import get_client


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
    def _image_tag_from_tar(tar_path: Path) -> str:
        with tarfile.open(tar_path, mode="r") as tf:
            member = tf.extractfile("manifest.json")
            if member is None:
                raise DmlRepoError("docker image tar missing manifest.json")
            manifest = json.loads(member.read())
        repo_tags = manifest[0].get("RepoTags") if manifest else None
        if not isinstance(repo_tags, list) or not repo_tags or not isinstance(repo_tags[0], str) or not repo_tags[0]:
            raise DmlRepoError("docker image tar missing RepoTags")
        return cast(str, repo_tags[0])

    @staticmethod
    def _prepare_image(runnable: dict[str, Any], workdir: Path, remote: dict[str, Any]) -> tuple[str, str | None]:
        image = DockerExecutor._image_input(runnable)
        if not is_s3_uri(image):
            return image, None
        tar_path = workdir / "image.tar"
        store = S3Store.from_remote_root(cast(str, remote["root"]))
        tar_path.write_bytes(store.get(image))
        image_ref = DockerExecutor._image_tag_from_tar(tar_path)
        DockerExecutor._run_docker("load", "-i", str(tar_path))
        return image_ref, image_ref

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
        try:
            image_ref, cleanup_image = self._prepare_image(runnable, workdir, remote)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

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

        container_id = self._run_docker(
            "run",
            "-d",
            *cast(list[str], runnable.get("kwargs", {}).get("flags", [])),
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

        return {
            "status": "retry",
            "error": None,
            "state": {
                "container_id": container_id,
                "cleanup_image": cleanup_image,
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
        container_id = state.get("container_id")

        if not isinstance(container_id, str) or not container_id:
            return {
                "status": "failure",
                "error": "docker poll: missing container_id in job state",
                "state": None,
            }

        docker_bin = shutil.which("docker")
        if docker_bin is None:
            return {
                "status": "failure",
                "error": "docker poll: docker executable not found",
                "state": None,
            }

        proc = subprocess.run(
            [docker_bin, "inspect", "--format", "{{.State.Status}}", container_id],
            check=False,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            container_status = "exited"
        else:
            container_status = proc.stdout.strip()

        if container_status in ("created", "running", "paused", "restarting"):
            return {"status": "retry", "error": None, "state": state}

        raw = _read_scratch_output(_scratch_uri(scratch_uri, "output.json"))
        if raw is not None:
            try:
                result = json.loads(raw)
                if result.get("status") in {"success", "retry"} or isinstance(result.get("error"), str):
                    return result
            except Exception as e:
                return {
                    "status": "failure",
                    "error": f"docker poll: could not read output: {e}",
                    "state": None,
                }

        return {
            "status": "failure",
            "error": f"docker container {container_id} exited without output",
            "state": None,
        }

    def cleanup(self, cache_key, execution_id, runnable, state, remote, scratch_uri, result_ref):
        del cache_key, execution_id, runnable, remote, scratch_uri, result_ref
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
        _cleanup_docker(container_id, state.get("cleanup_image"), docker_bin)
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
        argv_ptr: str | None = None,
    ) -> dict[str, Any]:
        del cache_key, execution_id, runnable, remote, scratch_uri, cancel_requested_by, argv_ptr
        state = state if isinstance(state, dict) else {}
        docker_bin = shutil.which("docker")
        container_id = state.get("container_id")
        if docker_bin is None:
            if isinstance(container_id, str) and container_id:
                return {"status": "failure", "error": "docker executable not found in PATH", "state": state}
            return {"status": "cancelled", "error": None, "state": state}
        if isinstance(container_id, str) and container_id:
            _cleanup_docker(container_id, state.get("cleanup_image"), docker_bin)
        return {"status": "cancelled", "error": None, "state": state}


def _cleanup_docker(container_id: str, cleanup_image: str | None, docker_bin: str) -> None:
    subprocess.run([docker_bin, "rm", "-f", container_id], check=False, capture_output=True, text=True)
    if isinstance(cleanup_image, str) and cleanup_image:
        subprocess.run([docker_bin, "image", "rm", "-f", cleanup_image], check=False, capture_output=True, text=True)
