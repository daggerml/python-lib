from __future__ import annotations

import json
import os
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Any, cast

from daggerml import Uri
from daggerml._internal.types import DmlRepoError, Runnable
from daggerml.contrib.executor_state import ExecutionRecord, ExecutionState
from daggerml.contrib.executors._base import ExecutorBase
from daggerml.contrib.s3 import S3Store, is_s3_uri

HEARTBEAT_STALENESS = 60.0


class DockerExecutor(ExecutorBase):
    name = "docker"
    adapter = "local"

    @staticmethod
    def _child_cache_key(cache_key: str) -> str:
        return f"{cache_key}:docker-child"

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

    @staticmethod
    def _worker_payload(
        runnable: Runnable, workdir: Path, *, argv_ptr: str, child_cache_key: str, remote: dict[str, Any]
    ) -> Path:
        if runnable.sub is None:
            raise DmlRepoError("docker executor requires sub runnable")
        payload: dict[str, Any] = {
            "runnable": DockerExecutor._encode_runnable(runnable.sub),
            "argv_ptr": argv_ptr,
            "cache_key": child_cache_key,
            "remote": remote,
        }
        input_path = workdir / "input.json"
        input_path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))
        return input_path

    @staticmethod
    def _terminal_child_state(child_cache_key: str) -> ExecutionRecord:
        child = ExecutionState(child_cache_key).get()
        if child is None:
            raise DmlRepoError(f"Docker nested execution missing child state for cache_key={child_cache_key!r}")
        if child["status"] not in {"succeeded", "failed"}:
            raise DmlRepoError(
                f"Docker nested execution reached terminal container status but child state is {child['status']!r}"
            )
        return child

    @staticmethod
    def _project_child_terminal(*, cache_key: str, child_cache_key: str) -> None:
        parent = ExecutionState(cache_key)
        child = DockerExecutor._terminal_child_state(child_cache_key)
        if not parent.lock():
            return
        try:
            if child["status"] == "succeeded":
                dag_id = child.get("dag_id")
                if not isinstance(dag_id, str) or not dag_id:
                    raise DmlRepoError("Docker nested execution succeeded without dag_id")
                parent.mark_succeeded(dag_id)
                return
            error = child.get("error")
            if not isinstance(error, str) or not error:
                error = "Docker nested execution failed without error"
            parent.mark_failed(error)
        finally:
            parent.unlock()

    @staticmethod
    def _docker_env(remote: dict[str, str]) -> list[str]:
        dynamodb_table = os.environ.get("DML_DYNAMODB_TABLE")
        if not dynamodb_table:
            raise DmlRepoError("docker executor requires DML_DYNAMODB_TABLE for nested execution state")
        env = {
            "DML_DYNAMODB_TABLE": dynamodb_table,
            "DML_REMOTE_ROOT": remote["root"],
        }
        for name in sorted(os.environ):
            if name.startswith("AWS_") and os.environ[name]:
                env[name] = os.environ[name]
        docker_env: list[str] = []
        for name, value in env.items():
            if value:
                docker_env.extend(["-e", f"{name}={value}"])
        return docker_env

    def start(
        self, *, cache_key: str, state: ExecutionRecord, runnable: Runnable, argv_ptr: str, remote: dict[str, str]
    ) -> None:
        es = ExecutionState(cache_key)
        child_cache_key = self._child_cache_key(cache_key)
        ExecutionState.upsert(child_cache_key, argv_ptr)
        workdir = Path(tempfile.mkdtemp(prefix=f"dml-docker-{cache_key}-"))
        image_ref, cleanup_image = self._prepare_image(runnable, workdir, remote)
        input_path = self._worker_payload(
            runnable,
            workdir,
            argv_ptr=argv_ptr,
            child_cache_key=child_cache_key,
            remote=remote,
        )

        docker_env = self._docker_env(remote)

        container_id = self._run_docker(
            "run",
            "-d",
            "-v",
            f"{workdir}:{workdir}",
            *cast(list[str], runnable.kwargs.get("flags", [])),
            *docker_env,
            image_ref,
            runnable.sub.adapter,
            "--poll",
            "-i",
            str(input_path),
            "-o",
            str(workdir / "output.json"),
        )

        assert es.lock()
        try:
            es.update_metadata(
                {
                    self.name: {
                        "child_cache_key": child_cache_key,
                        "container_id": container_id,
                        "workdir": str(workdir),
                        "cleanup_image": cleanup_image,
                    },
                }
            )
        finally:
            es.unlock()

    def poll(self, *, cache_key: str, state: ExecutionRecord) -> None:
        import time

        meta = (state.get("metadata") or {}).get(self.name, {})
        child_cache_key = meta.get("child_cache_key")
        if not isinstance(child_cache_key, str) or not child_cache_key:
            return
        child = ExecutionState(child_cache_key).get()
        if child is None:
            return
        if child["status"] in {"succeeded", "failed"}:
            try:
                self._project_child_terminal(cache_key=cache_key, child_cache_key=child_cache_key)
            except DmlRepoError as e:
                es = ExecutionState(cache_key)
                if es.lock():
                    try:
                        es.mark_failed(str(e))
                    finally:
                        es.unlock()
            return
        if child["heartbeat_ts"] is not None and child["heartbeat_ts"] + HEARTBEAT_STALENESS < time.time():
            msg = f"stale docker heartbeat (container ID: {meta.get('container_id')})"
            es = ExecutionState(cache_key)
            if es.lock():
                try:
                    es.mark_failed(msg)
                finally:
                    es.unlock()

    def cleanup(self, *, cache_key: str, state: ExecutionRecord) -> None:
        meta = (state.get("metadata") or {}).get(self.name, {})
        docker_bin = shutil.which("docker")
        if docker_bin is not None:
            container_id = meta.get("container_id")
            if isinstance(container_id, str) and container_id:
                subprocess.run([docker_bin, "rm", "-f", container_id], check=False, capture_output=True, text=True)
            cleanup_image = meta.get("cleanup_image")
            if isinstance(cleanup_image, str) and cleanup_image:
                subprocess.run(
                    [docker_bin, "image", "rm", "-f", cleanup_image], check=False, capture_output=True, text=True
                )
        workdir = meta.get("workdir")
        if isinstance(workdir, str) and workdir:
            shutil.rmtree(workdir, ignore_errors=True)
