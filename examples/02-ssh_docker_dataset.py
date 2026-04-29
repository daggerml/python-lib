"""Run the Docker dataset pipeline through SSH.

This example is the SSH-backed sibling of ``01-docker_dataset.py``. It starts a
local sshd that points back to the current machine, writes an env file for the
remote SSH session, builds the same Docker image from this repository, and then
executes the Docker-backed funks over SSH.
"""

from __future__ import annotations

import getpass
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
from urllib.parse import urlparse

import polars as pl

import daggerml as dml
from daggerml.contrib import api
from daggerml.contrib.funks import docker_build
from daggerml.contrib.s3 import S3Store

EXCLUDE_PATTERNS = (
    # ".git",  # we need .git to install lib from the repo
    "ignore/*",
    ".venv/*",
    ".mypy_cache/*",
    ".pytest_cache/*",
    "__pycache__/*",
    "*.pyc",
    "tests/*",
)
REPO_ROOT = Path(__file__).resolve().parents[1]


def _require_local_tools() -> None:
    missing = [name for name in ("docker", "ssh", "sshd", "ssh-keygen") if shutil.which(name) is None]
    if missing:
        raise RuntimeError(f"Missing required local tools: {', '.join(missing)}")


def _docker_run_flags() -> list[str]:
    flags: list[str] = []
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        parsed = urlparse(endpoint)
        if parsed.scheme == "http" and parsed.port is not None:
            flags.extend(
                [
                    "--add-host=host.docker.internal:host-gateway",
                    "-e",
                    f"AWS_ENDPOINT_URL=http://host.docker.internal:{parsed.port}",
                ]
            )
    for key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    ):
        value = os.environ.get(key)
        if value:
            flags.extend(["-e", f"{key}={value}"])
    return flags


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
        "DML_REMOTE_URI": os.environ["DML_REMOTE_URI"],
        **{k: v for k, v in os.environ.items() if k.startswith("AWS_")},
    }
    env_file.write_text(
        "\n".join(f"export {name}={shlex.quote(value)}" for name, value in sorted(exports.items())) + "\n"
    )
    return str(env_file)


@api.funkify(  # send over ssh
    uri="ssh",
    adapter="local",
    host=api.ref("ssh-host"),
    flags=api.ref("ssh-flags"),
    env_files=api.ref("ssh-env-files"),
)
@api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("dkr-flags"))  # run in docker
@api.funkify  # defaults to: run in a python subprocess
def download_dataset(dag):
    from sklearn.datasets import load_iris  # pyright:ignore[reportMissingImports] # noqa:F401

    return load_iris(as_frame=True).frame.dropna()


@api.funkify(
    uri="ssh",
    adapter="local",
    host=api.ref("ssh-host"),
    flags=api.ref("ssh-flags"),
    env_files=api.ref("ssh-env-files"),
)  # send over ssh
@api.funkify(uri="docker", image=api.ref("image"), flags=api.ref("dkr-flags"))  # run in docker
@api.funkify  # defaults to: run in a python subprocess
def predict_target(dag, dataset_uri):
    import io

    import pandas as pd  # pyright:ignore[reportMissingImports] # noqa:F401
    from sklearn.linear_model import LogisticRegression  # pyright:ignore[reportMissingImports] # noqa:F401

    from daggerml.contrib.s3 import S3Store

    payload = S3Store().get(dataset_uri.value())
    df = pd.read_parquet(io.BytesIO(payload))
    features = df.drop(columns=["target"])
    target = df["target"]
    model = LogisticRegression(max_iter=200)
    model.fit(features, target)
    out = df.copy()
    out["prediction"] = model.predict(features)
    return out


def main() -> None:
    _require_local_tools()
    sshd_proc = None
    try:
        import pandas  # pyright:ignore[reportMissingImports] # noqa:F401

        raise RuntimeError("pandas should not be installed in the local environment for this example to work")
    except ModuleNotFoundError:
        pass

    try:
        with TemporaryDirectory(prefix="daggerml-ssh-example-") as tmpdir:
            sshd_proc, ssh_flags, ssh_host = _start_local_sshd(tmpdir)
            ssh_env_file = _write_ssh_env_file(tmpdir)
            flags = _docker_run_flags()
            with dml.new("examples/02-ssh-docker-dataset") as dag:
                dag.dkr_build = docker_build
                s3 = S3Store()
                print("Creating Docker build context from repo root, excluding patterns:", EXCLUDE_PATTERNS)
                dkr_ctx = s3.tar(str(REPO_ROOT), excludes=EXCLUDE_PATTERNS, symlinks="ignore")
                dag.put(flags, name="dkr-flags")
                dag.put(ssh_host, name="ssh-host")
                dag.put(ssh_flags, name="ssh-flags")
                dag.put([ssh_env_file], name="ssh-env-files")
                print("Building Docker image (this may take a moment)...")
                t0 = time.time()
                dag.dkr_build(dkr_ctx, build_flags=["-f", "./examples/dkr-ctx/Dockerfile"], name="image")
                t1 = time.time()
                print("Re-building Docker image to demonstrate caching...")
                t2 = time.time()
                dag.dkr_build(dkr_ctx, build_flags=["-f", "./examples/dkr-ctx/Dockerfile"], name="image-redux")
                t3 = time.time()
                dag.download = download_dataset
                print("Loading dataset within Docker over SSH...")
                dataset = dag.download(name="dataset")
                print("Training model and generating predictions within Docker over SSH...")
                predictions = dag.call(predict_target, dataset, name="predictions")
                print("Committing DAG to persist artifacts...")
                dag.commit(predictions)
            print("Reading predictions parquet from S3...")
            df = pl.read_parquet(predictions.value().uri)
            print(f"Dataset parquet URI: {dataset.value()}")
            print(f"\nPredictions parquet URI: {predictions.value().uri}")
    finally:
        if sshd_proc is not None:
            sshd_proc.terminate()
            try:
                sshd_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                sshd_proc.kill()
    print("\nPredictions:")
    print(df.head())
    print(f"\nBuild times: {t1 - t0:.2f}s (cached: {t3 - t2:.2f}s)")


if __name__ == "__main__":
    main()
