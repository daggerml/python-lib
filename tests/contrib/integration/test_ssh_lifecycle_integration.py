"""Real local SSH transport with a Moto-backed remote and script worker."""

import getpass
import os
import shlex
import shutil
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor
from time import monotonic, sleep
from urllib.parse import urlparse

import boto3
import pytest

import daggerml.api as api
from daggerml import CancellationError
from daggerml.contrib.api import funkify
from daggerml.contrib.codecs import DelayedActionCodec

pytestmark = [pytest.mark.slow, pytest.mark.ssh, pytest.mark.serial]


@funkify(uri="script")
def remote_host_value(dag, value):
    from daggerml.contrib.s3 import S3Store

    store = S3Store()
    uri = store.put(data=b"ssh-worker-s3", suffix=".txt")
    assert store.get(uri) == b"ssh-worker-s3"
    return value.value() + 1


@funkify(uri="script")
def wait_on_ssh(dag, started, released):
    from time import sleep
    from urllib.parse import urlparse

    import boto3
    from botocore.exceptions import ClientError

    marker = urlparse(started.value())
    release = urlparse(released.value())
    client = boto3.client("s3")
    client.put_object(Bucket=marker.netloc, Key=marker.path.lstrip("/"), Body=b"ready")
    while True:
        try:
            client.head_object(Bucket=release.netloc, Key=release.path.lstrip("/"))
            return "released"
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in {"404", "NoSuchKey"}:
                raise
            sleep(0.1)


@pytest.fixture
def local_ssh(tmp_path, monkeypatch, remote_env, s3_bucket):
    del remote_env, s3_bucket
    monkeypatch.setattr(
        api, "_codecs",
        [(1, 1, DelayedActionCodec()), (0, 2, api.NodeCodec()), (0, 3, api.MiscPyTypeCodec())],
    )
    sshd = shutil.which("sshd") or ("/usr/sbin/sshd" if os.path.isfile("/usr/sbin/sshd") else None)
    if not sshd or not shutil.which("ssh") or not shutil.which("ssh-keygen"):
        pytest.skip("OpenSSH client, server and keygen are required")
    host_key = tmp_path / "host_key"
    user_key = tmp_path / "user_key"
    for key in (host_key, user_key):
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)], check=True, timeout=10)
    authorized = tmp_path / "authorized_keys"
    authorized.write_bytes(user_key.with_suffix(".pub").read_bytes())
    authorized.chmod(0o600)
    env_file = tmp_path / "worker.env"
    env_file.write_text("".join(
        f"export {key}={shlex.quote(value)}\n"
        for key, value in {**{key: os.environ[key] for key in (
            "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_ENDPOINT_URL",
        )}, "AWS_SHARED_CREDENTIALS_FILE": "/dev/null", "PATH": os.environ["PATH"]}.items()
    ))
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = tmp_path / "sshd_config"
    config.write_text(
        f"Port {port}\nListenAddress 127.0.0.1\nHostKey {host_key}\n"
        f"AuthorizedKeysFile {authorized}\nStrictModes no\n"
        "PasswordAuthentication no\nKbdInteractiveAuthentication no\n"
        f"PidFile {tmp_path / 'sshd.pid'}\n"
    )
    subprocess.run([sshd, "-t", "-f", str(config)], check=True, timeout=10)
    server = subprocess.Popen([sshd, "-D", "-e", "-f", str(config)], stderr=subprocess.PIPE, text=True)
    flags = ["-p", str(port), "-i", str(user_key), "-o", "BatchMode=yes", "-o", "ConnectTimeout=2",
             "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]
    try:
        deadline = monotonic() + 10
        while monotonic() < deadline:
            if server.poll() is not None:
                raise AssertionError(f"sshd exited: {server.stderr.read() if server.stderr else ''}")
            probe = subprocess.run(["ssh", *flags, f"{getpass.getuser()}@127.0.0.1", "true"],
                                   capture_output=True, text=True, timeout=5)
            if probe.returncode == 0:
                break
            sleep(0.1)
        else:
            raise AssertionError(f"local SSH did not become ready: {probe.stderr}")
        yield f"{getpass.getuser()}@127.0.0.1", flags, str(env_file)
    finally:
        server.terminate()
        try:
            server.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.communicate(timeout=5)


def test_ssh_executes_worker_with_environment_file_and_moto_s3(runtime_world, local_ssh):
    host, flags, env_file = local_ssh
    dml = runtime_world.publisher
    remote = funkify(remote_host_value, uri="ssh", host=host, flags=flags, env_files=[env_file])
    with api.new("ssh-result", dml=dml) as dag:
        result = dag.put(remote)(41, timeout=120_000)
        dag.commit(result)
    assert api.load("ssh-result", dml=dml).result.value() == 42
    cache_key = dml.dag.describe(result.context().ref)["cache_key"]
    execution = dml.cache.describe(cache_key)["execution"]
    record = dml.runtime.read_execution_record(execution)
    assert record["state"]["lifecycle"] == "succeeded"
    assert record["driver"]["cleanup"] is not None


def test_ssh_polls_and_cancels_remote_worker(runtime_world, local_ssh):
    host, flags, env_file = local_ssh
    dml = runtime_world.publisher
    wrapped = funkify(wait_on_ssh, uri="ssh", host=host, flags=flags, env_files=[env_file])
    dag = api.new("ssh-cancel", dml=dml)
    caller = dag.token
    worker = dag.put(wrapped)
    marker = f"{runtime_world.root}/started"
    release = f"{runtime_world.root}/released"

    def invoke():
        try:
            return worker(marker, release, timeout=120_000)
        except BaseException as exc:
            return exc

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(invoke)
        client = boto3.client("s3")
        parsed = urlparse(marker)
        deadline = monotonic() + 90
        while monotonic() < deadline:
            try:
                client.head_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
                break
            except client.exceptions.ClientError:
                sleep(0.25)
        else:
            raise AssertionError("SSH worker did not publish its S3 readiness marker")
        try:
            with pytest.raises(CancellationError):
                dag.cancel()
        finally:
            parsed_release = urlparse(release)
            client.put_object(Bucket=parsed_release.netloc, Key=parsed_release.path.lstrip("/"), Body=b"release")
        assert isinstance(future.result(timeout=30), BaseException)
    assert dml.runtime.read_execution_record(caller)["state"]["lifecycle"] == "canceled"
