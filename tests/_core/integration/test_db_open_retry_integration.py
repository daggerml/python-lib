"""Exercise environment-open recovery without adding hooks to production code."""

import shutil
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def retry_executable(tmp_path_factory):
    compiler = shutil.which("cc")
    if compiler is None:
        pytest.skip("C compiler is required for the native LMDB regression harness")
    root = Path(__file__).resolve().parents[3]
    build = tmp_path_factory.mktemp("db-open-retry")
    c = root / "c"
    lmdb = c / "third_party/lmdb/libraries/liblmdb"
    includes = ["-I", str(c), "-I", str(c / "include"), "-I", str(lmdb),
                "-I", str(c / "third_party/msgpack/include"), "-I", str(c / "third_party/sha256")]
    instrumented = build / "mdb.o"
    subprocess.run([compiler, "-O0", "-g", "-Dfcntl=test_fcntl", *includes,
                    "-c", str(lmdb / "mdb.c"), "-o", str(instrumented)],
                   check=True, capture_output=True, text=True, timeout=60)
    executable = build / "db-open-retry"
    sources = [Path(__file__).with_name("fixtures") / "db_open_retry.c", lmdb / "midl.c",
               *[c / "src" / name for name in ("dml_value.c", "dml_msgpack.c", "dml_hash.c")],
               c / "third_party/sha256/sha256.c",
               *[c / "third_party/msgpack/src" / name for name in (
                   "objectc.c", "unpack.c", "vrefbuffer.c", "zone.c", "version.c")]]
    subprocess.run([compiler, "-O0", "-g", "-pthread", *includes, *map(str, sources),
                    str(instrumented), "-o", str(executable)],
                   check=True, capture_output=True, text=True, timeout=60)
    return executable


@pytest.mark.parametrize("mode", ["transient", "persistent", "permission", "missing-data", "missing-dir"])
def test_environment_open_retry_is_bounded_and_narrow(retry_executable, tmp_path, mode):
    result = subprocess.run([str(retry_executable), mode, str(tmp_path)],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.skipif(sys.platform != "darwin", reason="LMDB uses named POSIX semaphores on macOS")
def test_environment_open_recovers_from_last_user_semaphore_unlink(retry_executable, tmp_path):
    result = subprocess.run([str(retry_executable), "race", str(tmp_path)],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "forced semaphore race recovered" in result.stdout
