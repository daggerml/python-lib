from __future__ import annotations

import json
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Any, cast

from daggerml import Uri
from daggerml._internal import DmlRepoError, ExecutionState, Runnable
from daggerml.contrib.executors._base import ExecutorBase
from daggerml.contrib.s3 import S3Store, is_s3_uri


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
    def _encode_value(value: Any) -> Any:
        if isinstance(value, Uri):
            return value.uri
        if isinstance(value, Runnable):
            return DockerExecutor._encode_runnable(value)
        if isinstance(value, dict):
            return {k: DockerExecutor._encode_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [DockerExecutor._encode_value(v) for v in value]
        if isinstance(value, tuple):
            return [DockerExecutor._encode_value(v) for v in value]
        return value

    @staticmethod
    def _encode_runnable(runnable: Runnable) -> dict[str, Any]:
        return {
            "target": runnable.target.uri,
            "adapter": runnable.adapter,
            "kwargs": DockerExecutor._encode_value(runnable.kwargs),
            "sub": None if runnable.sub is None else DockerExecutor._encode_runnable(runnable.sub),
        }

    @staticmethod
    def _image_input(runnable: Runnable) -> str:
        image = runnable.kwargs.get("image")
        if hasattr(image, "value") and callable(image.value):
            image = image.value()
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
    def _prepare_image(runnable: Runnable, workdir: Path, remote: dict[str, Any]) -> tuple[str, str | None]:
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
        runnable: Runnable,
        argv_ptr: str,
        remote: dict[str, str],
    ) -> dict[str, Any]:
        if runnable.sub is None:
            raise DmlRepoError("docker executor requires sub runnable")
        exec_state = ExecutionState(cache_key, remote_root=remote["root"])
        io = exec_state.adapter_io(execution_id, "local:docker")

        workdir = Path(tempfile.mkdtemp(prefix=f"dml-docker-{execution_id}-"))
        try:
            image_ref, cleanup_image = self._prepare_image(runnable, workdir, remote)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

        payload: dict[str, Any] = {
            "runnable": self._encode_runnable(runnable.sub),
            "argv_ptr": argv_ptr,
            "cache_key": cache_key,
            "execution_id": execution_id,
            "remote": remote,
            "state": None,
        }
        input_uri = io.write_input(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))

        container_id = self._run_docker(
            "run",
            "-d",
            *cast(list[str], runnable.kwargs.get("flags", [])),
            "-e",
            f"DML_REMOTE_URI={remote['root']}",
            image_ref,
            runnable.sub.adapter,
            "--poll",
            "-i",
            input_uri,
            "-o",
            io.output_uri,
        )

        return {
            "status": "running",
            "error": None,
            "state": {
                "container_id": container_id,
                "cleanup_image": cleanup_image,
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
        container_id = state.get("container_id")

        if not isinstance(container_id, str) or not container_id:
            return {"status": "failed", "error": "docker poll: missing container_id in job state"}

        docker_bin = shutil.which("docker")
        if docker_bin is None:
            return {"status": "failed", "error": "docker poll: docker executable not found"}

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
            return {"status": "running", "error": None, "state": state}

        # Container exited
        _cleanup_docker(container_id, state.get("cleanup_image"), docker_bin)

        exec_state = ExecutionState(cache_key, remote_root=remote["root"])
        io = exec_state.adapter_io(execution_id, "local:docker")
        raw = io.read_output()
        if raw is not None:
            try:
                result = json.loads(raw)
                if isinstance(result, dict) and result.get("status") in {"succeeded", "failed"}:
                    return result
            except Exception as e:
                return {"status": "failed", "error": f"docker poll: could not read output: {e}"}

        return {"status": "failed", "error": f"docker container {container_id} exited without output"}


def _cleanup_docker(container_id: str, cleanup_image: str | None, docker_bin: str) -> None:
    subprocess.run([docker_bin, "rm", "-f", container_id], check=False, capture_output=True, text=True)
    if isinstance(cleanup_image, str) and cleanup_image:
        subprocess.run([docker_bin, "image", "rm", "-f", cleanup_image], check=False, capture_output=True, text=True)
