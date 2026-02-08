from __future__ import annotations

import json
import shutil
import subprocess
import tarfile
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from daggerml import Uri
from daggerml._internal.types import DmlRepoError, Runnable
from daggerml.contrib.executor_state import LocalState
from daggerml.contrib.executors._base import ExecutorBase
from daggerml.contrib.s3 import S3Store, is_s3_uri


@dataclass
class DockerExecutor(ExecutorBase):
    runnable: Runnable | None = None
    workdir: Path | None = None
    name = "docker"
    adapter = "local"
    state_class = LocalState
    OWNER = "docker"
    docker_bin: str | None = field(default_factory=lambda: shutil.which("docker"))

    def __post_init__(self):
        if self.docker_bin is None:
            raise DmlRepoError("docker executable not found in PATH")

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

    def _worker_payload(self, *, argv_ptr: str, cache_key: str, remote: dict[str, Any]) -> Path:
        if self.workdir is None:
            raise DmlRepoError("docker executor worker payload requires workdir")
        if self.runnable.sub is None:
            raise DmlRepoError("docker executor requires sub runnable")
        state_dir = self.workdir / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "runnable": self._encode_runnable(self.runnable.sub),
            "argv_ptr": argv_ptr,
            "cache_key": cache_key,
            "remote": remote,
            "comms": {"kind": "local", "owner": self.OWNER, "spec": {"cache_dir": str(state_dir)}},
        }
        input_path = self.workdir / "input.json"
        input_path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))
        return input_path

    def _nested_state(self, metadata: dict[str, Any]) -> Any:
        state_dir = metadata.get("state_dir")
        nested_cache_key = metadata.get("nested_cache_key")
        if not isinstance(state_dir, str) or not isinstance(nested_cache_key, str):
            return None
        return LocalState(nested_cache_key, cache_dir=state_dir).get()

    def _metadata(self, record: dict[str, Any]) -> dict[str, Any]:
        return cast(dict[str, Any], cast(dict[str, Any], record.get("metadata", {})).get(self.OWNER, {}))

    def _read_output(self, metadata: dict[str, Any]) -> dict[str, Any] | None:
        output_path = metadata.get("output_path")
        if not isinstance(output_path, str) or not Path(output_path).exists():
            return None
        result = json.loads(Path(output_path).read_text())
        if not isinstance(result, dict):
            raise DmlRepoError("docker adapter output must be a dict")
        return result

    def _record_result(self, state: Any, record: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
        next_record = state.update_status(
            status=cast(str, result.get("status")),
            error=cast(str | None, result.get("error")),
            owner_executor=cast(str | None, record.get("owner_executor")),
            owner_instance=cast(str | None, record.get("owner_instance")),
            heartbeat_ts=cast(float | None, record.get("heartbeat_ts")),
            lease_expires_ts=None,
        )
        state.update(next_record)
        return {"status": result.get("status"), "error": result.get("error")}

    def _finish(
        self, *, state: Any, record: dict[str, Any], metadata: dict[str, Any], result: dict[str, Any]
    ) -> dict[str, Any]:
        response = self._record_result(state, record, result)
        self._cleanup(metadata=metadata)
        return response

    def _fail(self, *, state: Any, record: dict[str, Any], metadata: dict[str, Any], error: str) -> dict[str, Any]:
        response = self._record_result(state, record, {"status": "failed", "error": error})
        self._cleanup(metadata=metadata)
        return response

    def _is_terminal(self, nested: Any) -> bool:
        return isinstance(nested, dict) and nested.get("status") in {"succeeded", "failed", "canceled"}

    def _is_stale(self, nested: Any) -> bool:
        if not isinstance(nested, dict):
            return False
        lease_expires_ts = nested.get("lease_expires_ts")
        return isinstance(lease_expires_ts, (int, float)) and lease_expires_ts < time.time()

    def _container_status(self, container_id: str) -> str:
        return self._run_docker("inspect", "-f", "{{.State.Status}}", container_id, check=False) or "missing"

    def _container_exit_code(self, container_id: str) -> int:
        raw = self._run_docker("inspect", "-f", "{{.State.ExitCode}}", container_id)
        try:
            return int(raw)
        except ValueError as e:
            raise DmlRepoError(f"docker inspect returned invalid exit code: {raw}") from e

    def _container_logs(self, container_id: str) -> str:
        return self._run_docker("logs", container_id, check=False)

    def _cleanup(self, *, metadata: dict[str, Any]) -> None:
        container_id = metadata.get("container_id")
        if isinstance(container_id, str) and container_id:
            self._run_docker("rm", "-f", container_id, check=False)
        cleanup_image = metadata.get("cleanup_image")
        if isinstance(cleanup_image, str) and cleanup_image:
            self._run_docker("image", "rm", "-f", cleanup_image, check=False)
        workdir = metadata.get("workdir")
        if isinstance(workdir, str) and workdir:
            shutil.rmtree(workdir, ignore_errors=True)

    @classmethod
    def start(cls, *, runnable, argv_ptr, cache_key, remote, state=None):
        safe_cache_key = "".join(ch if ch.isalnum() else "-" for ch in cache_key[:32])
        workdir = Path(tempfile.mkdtemp(prefix=f"dml-docker-{safe_cache_key}-"))
        return cls(runnable=runnable, workdir=workdir)._start(
            argv_ptr=argv_ptr, cache_key=cache_key, remote=remote, state=state
        )

    def _start(self, *, argv_ptr, cache_key, remote, state=None):
        if state is None:
            raise DmlRepoError("docker start requires locked state")
        record = state.get()
        if record is not None:
            status = record.get("status")
            if status in {"succeeded", "failed", "pending", "running", "canceled"}:
                return {"status": status, "error": record.get("error")}
        if self.runnable is None or self.workdir is None:
            raise DmlRepoError("docker executor start requires runnable and workdir")
        if self.runnable.sub is None:
            raise DmlRepoError("docker executor requires sub runnable")
        output_path = self.workdir / "output.json"
        image_ref, cleanup_image = self._prepare_image(remote=remote)
        input_path = self._worker_payload(argv_ptr=argv_ptr, cache_key=cache_key, remote=remote)
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
        now = time.time()
        created = state.put_if_absent(
            state.init_record(
                status="running",
                error=None,
                owner_executor=self.OWNER,
                owner_instance=f"container:{container_id}",
                heartbeat_ts=now,
                lease_expires_ts=None,
            )
        )
        metadata = {
            "container_id": container_id,
            "workdir": str(self.workdir),
            "output_path": str(output_path),
            "state_dir": str(self.workdir / "state"),
            "nested_cache_key": cache_key,
            "cleanup_image": cleanup_image,
        }
        if created:
            state.update(state.set_executor_metadata(executor_id=self.OWNER, data=metadata))
        else:
            self._cleanup(metadata=metadata)
        return {"status": "running", "error": None}

    @classmethod
    def poll(cls, *, state=None):
        return cls()._poll(state=state)

    def _poll(self, *, state=None):
        if state is None:
            raise DmlRepoError("docker poll requires locked state")
        record = state.get()
        if record is None:
            return {"status": "pending", "error": None}
        status = record.get("status")
        if status in {"succeeded", "failed", "canceled"}:
            return {"status": status, "error": record.get("error")}
        metadata = self._metadata(cast(dict[str, Any], record))
        container_id = metadata.get("container_id")
        if not isinstance(container_id, str) or not container_id:
            return {"status": "pending", "error": None}
        nested = self._nested_state(metadata)
        if self._is_terminal(nested):
            output = self._read_output(metadata)
            if output is None:
                return self._fail(
                    state=state,
                    record=cast(dict[str, Any], record),
                    metadata=metadata,
                    error="docker nested execution reached terminal state without output",
                )
            return self._finish(state=state, record=cast(dict[str, Any], record), metadata=metadata, result=output)

        if self._is_stale(nested):
            return self._fail(
                state=state,
                record=cast(dict[str, Any], record),
                metadata=metadata,
                error="Docker nested execution heartbeat stale",
            )

        container_status = self._container_status(container_id)
        if container_status in {"created", "running", "restarting"}:
            return {"status": "running", "error": None}

        output = self._read_output(metadata)
        if output is not None:
            return self._finish(state=state, record=cast(dict[str, Any], record), metadata=metadata, result=output)

        exit_code = self._container_exit_code(container_id)
        logs = self._container_logs(container_id)
        return self._fail(
            state=state,
            record=cast(dict[str, Any], record),
            metadata=metadata,
            error=f"Docker container exited with code {exit_code}: {logs}".strip(),
        )

    @classmethod
    def kill(cls, *, state=None):
        return cls()._kill(state=state)

    def _kill(self, *, state=None):
        if state is None:
            raise DmlRepoError("docker kill requires locked state")
        record = state.get()
        if record is None:
            return {"status": "canceled", "error": None}
        status = record.get("status")
        if status in {"succeeded", "failed", "canceled"}:
            return {"status": status, "error": record.get("error")}
        metadata = cast(dict[str, Any], cast(dict[str, Any], record.get("metadata", {})).get(self.OWNER, {}))
        self._cleanup(metadata=metadata)
        canceled = state.update_status(
            status="canceled",
            error=None,
            owner_executor=cast(str | None, record.get("owner_executor")),
            owner_instance=cast(str | None, record.get("owner_instance")),
            heartbeat_ts=cast(float | None, record.get("heartbeat_ts")),
            lease_expires_ts=None,
        )
        state.update(canceled)
        return {"status": "canceled", "error": None}

    @classmethod
    def gc(cls, *, state=None):
        return cls()._gc(state=state)

    def _gc(self, *, state=None):
        if state is None:
            raise DmlRepoError("docker gc requires locked state")
        record = state.get()
        if record is None:
            return None
        metadata = cast(dict[str, Any], cast(dict[str, Any], record.get("metadata", {})).get(self.OWNER, {}))
        self._cleanup(metadata=metadata)
        return None
