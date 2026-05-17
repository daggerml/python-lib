"""Start a local moto server and write sourceable AWS env vars.

This helper mirrors the local moto setup used across examples: it starts moto on
an ephemeral port, creates the example bucket, writes a shell env file, and
keeps running until Ctrl-C.
"""

from __future__ import annotations

import os
import shlex
import tempfile
import time
from pathlib import Path

import boto3


def _write_env_file(env_values: dict[str, str]) -> Path:
    fd, path_str = tempfile.mkstemp(prefix="daggerml-moto-", suffix=".env")
    path = Path(path_str)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write("# Source this file to configure local moto-backed DaggerML examples.\n")
        for key, value in env_values.items():
            f.write(f"export {key}={shlex.quote(value)}\n")
    return path


def main() -> None:
    for var in os.environ.keys():
        if var.startswith("AWS_") or var.startswith("DML_"):
            del os.environ[var]
    try:
        from moto.server import ThreadedMotoServer
    except ModuleNotFoundError as e:
        raise RuntimeError("Install moto[server] to run this helper: pip install 'moto[server]'") from e

    server = ThreadedMotoServer(port=0, verbose=False)
    env_file: Path | None = None
    try:
        server.start()
        host, port = server.get_host_and_port()
        endpoint = f"http://{host}:{port}"

        env_values = {
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_REGION": "us-east-1",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
            "AWS_ENDPOINT_URL": endpoint,
            "DML_REMOTE_ROOT": "s3://daggerml-example/artifacts",
        }

        for key, value in env_values.items():
            os.environ[key] = value

        boto3.client("s3", endpoint_url=endpoint).create_bucket(Bucket="daggerml-example")
        env_file = _write_env_file(env_values)

        print("Moto server started.")
        print(f"  Endpoint: {endpoint}")
        print("  Bucket: daggerml-example")
        print(f"  DML_REMOTE_ROOT: {env_values['DML_REMOTE_ROOT']}")
        print()
        print(f"Env file written: {env_file}")
        print(f"Source it with: source {env_file}")
        print("\nPress Ctrl-C to stop moto and clean up.")

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nCtrl-C received, shutting down...")
    finally:
        if env_file is not None and env_file.exists():
            env_file.unlink()
            print(f"Deleted env file: {env_file}")
        server.stop()
        print("Moto server stopped.")


if __name__ == "__main__":
    main()
