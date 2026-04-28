from __future__ import annotations

import getpass
import logging
import os
import shlex
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Any, cast

import pytest

from daggerml import Dml, Uri
from daggerml._internal.types import Runnable
from daggerml.contrib import adapter_registry as areg
from daggerml.contrib import api
from daggerml.contrib import executor_registry as ereg
from daggerml.contrib.adapters import LocalAdapter
from daggerml.contrib.executors import ScriptExecutor, SshExecutor

logger = logging.getLogger(__name__)


def _require_ssh_tools() -> None:
    missing = [name for name in ("ssh", "sshd", "ssh-keygen") if shutil.which(name) is None]
    if missing:
        pytest.skip(f"missing ssh tools: {', '.join(missing)}")


@pytest.fixture(autouse=True)
def _reset_registries(tmp_path, monkeypatch):
    areg._reset_for_tests()
    ereg._reset_for_tests()
    monkeypatch.setenv("DML_TEST_FN_STATE_DIR", str(tmp_path / "state"))
    areg.register_adapter(LocalAdapter)
    ereg.register_executor(ScriptExecutor)
    ereg.register_executor(SshExecutor)
    yield
    areg._reset_for_tests()
    ereg._reset_for_tests()


@pytest.fixture
def local_sshd():
    _require_ssh_tools()
    sshd_proc = None
    with TemporaryDirectory(prefix="daggerml-ssh-test-") as tmpd:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()

        host_key_path = Path(tmpd) / "ssh_host_ed25519_key"
        client_key_path = Path(tmpd) / "client_ed25519_key"
        authorized_keys_path = Path(tmpd) / "authorized_keys"
        sshd_config_path = Path(tmpd) / "sshd_config"
        pid_file = Path(tmpd) / "sshd.pid"

        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(host_key_path)], check=True)
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(client_key_path)], check=True)

        shutil.copyfile(client_key_path.with_suffix(".pub"), authorized_keys_path)
        authorized_keys_path.chmod(0o600)

        sshd_config_path.write_text(
            dedent(
                f"""
                Port {port}
                ListenAddress 127.0.0.1
                HostKey {host_key_path}
                PidFile {pid_file}
                LogLevel VERBOSE
                StrictModes no
                PasswordAuthentication no
                KbdInteractiveAuthentication no
                ChallengeResponseAuthentication no
                PubkeyAuthentication yes
                AuthorizedKeysFile {authorized_keys_path}
                UsePAM no
                PermitRootLogin no
                """
            ).strip()
            + "\n"
        )

        sshd_path = shutil.which("sshd")
        assert sshd_path is not None
        sshd_proc = subprocess.Popen(
            [sshd_path, "-D", "-e", "-f", str(sshd_config_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        flags = [
            "-i",
            str(client_key_path),
            "-p",
            str(port),
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "IdentitiesOnly=yes",
        ]
        logger.debug(
            "starting local sshd tmpdir=%s port=%s host_key=%s client_key=%s",
            tmpd,
            port,
            host_key_path,
            client_key_path,
        )

        deadline = time.time() + 5.0
        while time.time() < deadline:
            if sshd_proc.poll() is not None:
                stdout, stderr = sshd_proc.communicate(timeout=1)
                pytest.skip(
                    "local sshd failed to start:\n"
                    f"stdout: {stdout.decode(errors='replace')}\n"
                    f"stderr: {stderr.decode(errors='replace')}"
                )
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                    break
            except OSError:
                time.sleep(0.1)
        else:
            sshd_proc.terminate()
            pytest.skip("timeout waiting for local sshd to start")

        try:
            logger.debug("local sshd ready host=%s flags=%s", f"{getpass.getuser()}@127.0.0.1", flags)
            yield flags, f"{getpass.getuser()}@127.0.0.1"
        finally:
            sshd_proc.terminate()
            try:
                sshd_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                sshd_proc.kill()
            logger.debug("local sshd stopped")


@pytest.fixture
def ssh_resource_data(local_sshd, tmp_path):
    flags, host = local_sshd
    env_file = tmp_path / "ssh.env"
    remote_state_dir = tmp_path / "remote-state"
    remote_state_dir.mkdir()
    aws_exports = "\n".join(
        f"export {name}={shlex.quote(value)}" for name, value in sorted(os.environ.items()) if name.startswith("AWS_")
    )
    # required for gh actions sanitize task
    sanitizer_exports = "\n".join(
        f"export {name}={shlex.quote(os.environ[name])}"
        for name in ("LD_PRELOAD", "ASAN_OPTIONS", "UBSAN_OPTIONS")
        if name in os.environ
    )
    env_file.write_text(
        dedent(
            f"""
            export DML_TEST_FN_STATE_DIR={shlex.quote(str(remote_state_dir))}
            export PATH={shlex.quote(str(Path(sys.executable).parent))}:$PATH
            export DML_TEST_SSH_VALUE=ssh-ok
            {aws_exports}
            {sanitizer_exports}
            """
        ).strip()
        + "\n"
    )
    return {"host": host, "flags": flags, "env_files": [str(env_file)]}


def _remote() -> dict[str, str]:
    return {"root": os.environ["DML_REMOTE_URI"]}


def _mk_argv_ptr(*args: Any, argv0: Any | None = None) -> str:
    with Dml.temporary() as dml:
        dag = dml.new("argv-src", "argv-src")
        index_ref = dag._require_index_ref()
        head = argv0 if argv0 is not None else Runnable(target=Uri("daggerml:list"), kwargs={}, adapter="")
        fn_ref = dml.index.put_literal(index_ref, head)
        arg_refs = [dml.index.put_literal(index_ref, value) for value in args]
        with dml.index._tx(readonly=False) as txn:
            argv_ref = dml.index._prepare_fn(index_ref, [fn_ref, *arg_refs], {}, txn)
        return dml.index._remote_ops().put_ref_manifest(argv_ref)


def _poll_until_terminal(*, runnable: Runnable, argv_ptr: str, cache_key: str) -> dict[str, Any]:
    execution_id = f"exec-{cache_key}"
    state: dict[str, Any] | None = None
    for _ in range(200):
        result = LocalAdapter.send(
            runnable=runnable,
            argv_ptr=argv_ptr,
            cache_key=cache_key,
            execution_id=execution_id,
            remote=_remote(),
            state=state,
        )
        logger.debug(
            "ssh integration poll cache_key=%s execution_id=%s status=%s error=%r state=%r",
            cache_key,
            execution_id,
            result.get("status"),
            result.get("error"),
            result.get("state"),
        )
        if state is None and result.get("status") == "running":
            state = cast(dict[str, Any], result.get("state"))
        if result["status"] in {"succeeded", "failed"}:
            return cast(dict[str, Any], result)
        time.sleep(0.05)
    pytest.fail("ssh executor did not reach terminal state")


def test_ssh_executor_integration_runs_script_over_local_sshd(ssh_resource_data):
    decorate = api.funkify(uri="ssh", adapter="local", **ssh_resource_data)

    @decorate
    @api.funkify(uri="script", adapter="local")
    def fn(dag):
        import os

        return os.environ["DML_TEST_SSH_VALUE"]

    with Dml.temporary() as dml:
        dag = dml.new("ssh-int", "ssh-int")
        runnable = cast(Runnable, dag.put(cast(Any, fn)).value())

        argv_ptr = _mk_argv_ptr(argv0=runnable)
        result = _poll_until_terminal(runnable=runnable, argv_ptr=argv_ptr, cache_key="ck-ssh-int-success")
    assert result["status"] == "succeeded"
    assert result["error"] is None
    assert isinstance(result.get("dag_id"), str)
