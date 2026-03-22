from __future__ import annotations

import json
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from daggerml import Uri
from daggerml._internal.types import DmlRepoError, Runnable
from daggerml.contrib.executor_state import LocalState, is_stale
from daggerml.contrib.executors._base import ExecutorBase
from daggerml.contrib.s3 import S3Store, is_s3_uri


@dataclass
class DockerExecutor(ExecutorBase):
    workdir: Path
    state: LocalState
    runnable: Runnable | None = None
    name = "docker"
    adapter = "local"
    state_class = LocalState
    docker_bin: str | None = field(default_factory=lambda: shutil.which("docker"))

    def __post_init__(self):
        if isinstance(self.workdir, str):
            self.workdir = Path(self.workdir)
        if self.docker_bin is None:
            raise DmlRepoError("docker executable not found in PATH")

    @property
    def cache_key(self) -> str:
        return self.state.cache_key

    @classmethod
    def resolve_runnable(cls, uri, kwargs, sub):
        # kwargs, uri, or sub may be a delayed action or Node or literal
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

    def _run_docker(self, *args: str, check: bool = True) -> str:
        assert self.docker_bin is not None
        proc = subprocess.run([self.docker_bin, *args], check=False, capture_output=True, text=True)
        if proc.returncode == 0:
            return proc.stdout.strip() or proc.stderr.strip()
        if check:
            command = f"{self.docker_bin} {' '.join(args)}"
            raise DmlRepoError(
                f"docker command failed ({proc.returncode}): {command}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
            )
        return proc.stdout.strip() or proc.stderr.strip()

    def _encode_value(self, value: Any) -> Any:
        if isinstance(value, Uri):
            return value.uri
        if isinstance(value, Runnable):
            return self._encode_runnable(value)
        if isinstance(value, dict):
            return {k: self._encode_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._encode_value(v) for v in value]
        if isinstance(value, tuple):
            return [self._encode_value(v) for v in value]
        return value

    def _encode_runnable(self, runnable: Runnable) -> dict[str, Any]:
        return {
            "target": runnable.target.uri,
            "adapter": runnable.adapter,
            "kwargs": self._encode_value(runnable.kwargs),
            "sub": None if runnable.sub is None else self._encode_runnable(runnable.sub),
        }

    def _image_input(self, runnable: Runnable) -> str:
        image = runnable.kwargs.get("image")
        if hasattr(image, "value") and callable(image.value):
            image = image.value()
        if isinstance(image, Uri):
            return image.uri
        if isinstance(image, str) and image:
            return image
        raise DmlRepoError("docker executor image must resolve to a non-empty Uri or string")

    def _image_tag_from_tar(self, tar_path: Path) -> str:
        with tarfile.open(tar_path, mode="r") as tf:
            member = tf.extractfile("manifest.json")
            if member is None:
                raise DmlRepoError("docker image tar missing manifest.json")
            manifest = json.loads(member.read())
        repo_tags = manifest[0].get("RepoTags") if manifest else None
        if not isinstance(repo_tags, list) or not repo_tags or not isinstance(repo_tags[0], str) or not repo_tags[0]:
            raise DmlRepoError("docker image tar missing RepoTags")
        return cast(str, repo_tags[0])

    def _prepare_image(self, *, remote: dict[str, Any]) -> tuple[str, str | None]:
        if self.workdir is None or self.runnable is None:
            raise DmlRepoError("docker executor image preparation requires runnable and workdir")
        image = self._image_input(self.runnable)
        if not is_s3_uri(image):
            return image, None
        tar_path = self.workdir / "image.tar"
        store = S3Store.from_remote_root(cast(str, remote["root"]))
        tar_path.write_bytes(store.get(image))
        image_ref = self._image_tag_from_tar(tar_path)
        self._run_docker("load", "-i", str(tar_path))
        return image_ref, image_ref

    def _worker_payload(self, *, argv_ptr: str, remote: dict[str, Any]) -> Path:
        if self.runnable.sub is None:
            raise DmlRepoError("docker executor requires sub runnable")
        state_dir = self.workdir / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "runnable": self._encode_runnable(self.runnable.sub),
            "argv_ptr": argv_ptr,
            "cache_key": self.cache_key,
            "remote": remote,
            "comms": {"kind": "local", "spec": {"cache_dir": str(state_dir)}},
        }
        input_path = self.workdir / "input.json"
        input_path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))
        return input_path

    def _nested_state(self, metadata: dict[str, Any]) -> Any:
        nested_state = LocalState(self.cache_key, cache_dir=metadata["state_dir"])
        return nested_state.get()

    @classmethod
    def start(cls, *, runnable, argv_ptr, cache_key, remote, state):
        workdir = Path(tempfile.mkdtemp(prefix=f"dml-docker-{cache_key}-"))
        return cls(workdir=workdir, state=state, runnable=runnable)._start(
            argv_ptr=argv_ptr, remote=remote, state=state
        )

    def _start(self, *, argv_ptr, remote, state=None):
        output_path = self.workdir / "output.json"
        image_ref, cleanup_image = self._prepare_image(remote=remote)
        input_path = self._worker_payload(argv_ptr=argv_ptr, remote=remote)
        container_id = self._run_docker(
            "run",
            "-d",
            "-v",
            f"{self.workdir}:{self.workdir}",
            *cast(list[str], self.runnable.kwargs.get("flags", [])),
            "-e",
            f"DML_REMOTE_ROOT={remote['root']}",
            "-e",
            f"DML_REMOTE_CACHE={remote['cache']}",
            image_ref,
            self.runnable.sub.adapter,
            "--poll",
            "-i",
            str(input_path),
            "-o",
            str(output_path),
        )
        assert state.put_if_absent(state.init_record(status="running", error=None))
        metadata = {
            "container_id": container_id,
            "workdir": str(self.workdir),
            "output_path": str(output_path),
            "state_dir": str(self.workdir / "state"),
            "cleanup_image": cleanup_image,
        }
        state.update(state.set_executor_metadata(self.name, data=metadata))
        return {"status": "running", "error": None}

    @classmethod
    def poll(cls, state):
        metadata = state.get_executor_metadata(cls.name)
        return cls(workdir=Path(metadata["workdir"]), state=state)._poll(state=state)

    def _poll(self, state):
        metadata = state.get_executor_metadata(self.name)
        nested = self._nested_state(metadata)
        if nested["status"] in {"succeeded", "failed", "canceled"}:
            return {"status": nested["status"], "error": nested.get("error")}
        if is_stale(nested):
            msg = f"stale docker heartbeat (container ID: {metadata.get('container_id')})"
            return {"status": "failed", "error": msg}
        return {"status": "running", "error": None}

    @classmethod
    def gc(cls, state):
        metadata = state.get_executor_metadata(cls.name)
        docker_bin = shutil.which("docker")
        if docker_bin is not None:
            container_id = metadata.get("container_id")
            if isinstance(container_id, str) and container_id:
                subprocess.run([docker_bin, "rm", "-f", container_id], check=False, capture_output=True, text=True)
            cleanup_image = metadata.get("cleanup_image")
            if isinstance(cleanup_image, str) and cleanup_image:
                subprocess.run(
                    [docker_bin, "image", "rm", "-f", cleanup_image], check=False, capture_output=True, text=True
                )
        workdir = metadata.get("workdir")
        if isinstance(workdir, str) and workdir:
            shutil.rmtree(workdir, ignore_errors=True)
        state.delete()
        return None
