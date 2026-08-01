"""Run the Docker dataset pipeline through SSH.

This example is the SSH-backed sibling of ``01-docker_dataset.py``. It starts a
local sshd that points back to the current machine, writes an env file for the
remote SSH session, builds the same Docker image from this repository, and then
executes the Docker-backed funks over SSH.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import shlex
import shutil
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import NamedTuple

import daggerml as dml
from daggerml.contrib import api

REPO_ROOT = Path(__file__).resolve().parents[2]


class SshServer(NamedTuple):
    host: str
    flags: list[str]
    env_file: str


def _require_local_tools() -> None:
    missing = [name for name in ("docker", "ssh", "sshd", "ssh-keygen") if shutil.which(name) is None]
    if missing:
        raise RuntimeError(f"Missing required local tools: {', '.join(missing)}")


def _start_local_sshd(tmpdir: str) -> tuple[subprocess.Popen[bytes], list[str], str]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    host_key_path = Path(tmpdir) / "ssh_host_ed25519_key"
    client_key_path = Path(tmpdir) / "client_ed25519_key"
    authorized_keys_path = Path(tmpdir) / "authorized_keys"
    sshd_config_path = Path(tmpdir) / "sshd_config"
    pid_file = Path(tmpdir) / "sshd.pid"

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

    deadline = time.time() + 5.0
    while time.time() < deadline:
        if sshd_proc.poll() is not None:
            stdout, stderr = sshd_proc.communicate(timeout=1)
            raise RuntimeError(
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
        raise RuntimeError("timeout waiting for local sshd to start")

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
    host = f"{getpass.getuser()}@127.0.0.1"
    return sshd_proc, flags, host


def _write_ssh_env_file(tmpdir: str) -> str:
    env_file = Path(tmpdir) / "ssh.env"
    exports = {
        "PATH": f"{Path(sys.executable).parent}:{os.environ.get('PATH', '')}",
        "UV_PROJECT": str(REPO_ROOT),
        "DML_REMOTE_ROOT": os.environ["DML_REMOTE_ROOT"],
        **{k: v for k, v in os.environ.items() if k.startswith("AWS_")},
    }
    env_file.write_text(
        "\n".join(f"export {name}={shlex.quote(value)}" for name, value in sorted(exports.items())) + "\n"
    )
    return str(env_file)


@contextmanager
def ssh_server(dag):
    sshd_proc = None
    try:
        with TemporaryDirectory(prefix="daggerml-ssh-example-") as tmpdir:
            sshd_proc, ssh_flags, ssh_host = _start_local_sshd(tmpdir)
            ssh_env_file = _write_ssh_env_file(tmpdir)
            dag.put(ssh_host, name="ssh-host")
            dag.put(ssh_flags, name="ssh-flags")
            dag.put([ssh_env_file], name="ssh-env-files")
            yield SshServer(ssh_host, ssh_flags, ssh_env_file)
            return
    finally:
        if sshd_proc is not None:
            sshd_proc.terminate()
            try:
                sshd_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                sshd_proc.kill()


@api.funkify(
    uri="ssh",
    adapter="local",
    host=api.ref("ssh-host"),
    flags=api.ref("ssh-flags"),
    env_files=api.ref("ssh-env-files"),
)  # send over ssh
@api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("dkr-flags"))  # run in docker
@api.funkify  # defaults to: run in a python subprocess
def predict_target(dag, dataset, params):
    import pandas as pd  # pyright:ignore[reportMissingImports] # noqa:F401
    from sklearn.metrics import r2_score  # pyright:ignore[reportMissingImports] # noqa:F401
    from sklearn.model_selection import train_test_split  # pyright:ignore[reportMissingImports] # noqa:F401
    from sklearn.neighbors import KNeighborsClassifier  # pyright:ignore[reportMissingImports] # noqa:F401

    df = pd.read_parquet(dataset.value().uri)
    X = df.drop(columns=["target"])
    y = df["target"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=42)
    model = KNeighborsClassifier(**params.value())
    model.fit(X_train, y_train)
    train_r2 = r2_score(y_train, model.predict(X_train))
    test_r2 = r2_score(y_test, model.predict(X_test))
    return {"train": train_r2, "test": test_r2}


def main(dag_name: str, docker_dag_name: str) -> None:
    _require_local_tools()
    dag = dml.new(name=dag_name)
    loaded_dag = dml.load(docker_dag_name)
    dag.image = loaded_dag.image
    dag.dataset = loaded_dag.dataset
    dag.put(loaded_dag["dkr-flags"], name="dkr-flags")
    with ssh_server(dag):
        print("Training model and generating predictions within Docker over SSH...")
        predictions = dag.call(predict_target, dag.dataset, {}, name="predictions")
        print("Committing DAG to persist artifacts...")
    dag.commit(predictions)
    print("Reading predictions parquet from S3...")
    print(json.dumps(predictions.value(), indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dag_name")
    parser.add_argument("docker_dag_name")
    args = parser.parse_args()
    main(args.dag_name, args.docker_dag_name)
