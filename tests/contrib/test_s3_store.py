from __future__ import annotations

import io
import stat
import sys
import tarfile
from pathlib import Path

import pytest

from daggerml import Uri
from daggerml._internal.types import DmlRepoError
from daggerml.contrib.s3 import S3Store, is_s3_uri


def test_s3_store_default_uses_remote_root_data_prefix():
    store = S3Store()
    uri = store.put(data=b"hello")
    assert isinstance(uri, Uri)
    assert uri.uri.startswith("s3://test-bucket/test-prefix/data/")


def test_s3_store_parse_uri_and_name_to_uri():
    store = S3Store(bucket="test-bucket", prefix="base")
    assert store.parse_uri("s3://other/key") == ("other", "key")
    assert store.parse_uri("x") == ("test-bucket", "base/x")
    assert store._name2uri("x") == Uri("s3://test-bucket/base/x")


def test_s3_store_put_get_exists_ls_rm_roundtrip():
    store = S3Store()
    uri = store.put(data=b"abc", suffix=".txt")
    assert isinstance(uri, Uri)
    assert store.exists(uri) is True
    assert store.get(uri) == b"abc"
    listed = store.ls(recursive=True)
    assert uri in listed
    store.rm(uri)
    assert store.exists(uri) is False


def test_s3_store_put_js_get_js_roundtrip():
    store = S3Store()
    uri = store.put_js({"b": 2, "a": 1})
    assert isinstance(uri, Uri)
    assert uri.uri.endswith(".json")
    assert store.get_js(uri) == {"a": 1, "b": 2}


def test_s3_store_tar_and_untar(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("A")
    (src / "run.sh").write_text("#!/bin/sh\necho hi\n")
    (src / "b.tmp").write_text("TMP")
    (src / "run.sh").chmod(0o755)

    store = S3Store()
    tar_uri = store.tar(src, excludes=["*.tmp"])
    out = tmp_path / "out"
    store.untar(tar_uri, out)

    assert (out / "a.txt").read_text() == "A"
    assert not (out / "b.tmp").exists()
    src_mode = (src / "run.sh").stat().st_mode
    out_mode = (out / "run.sh").stat().st_mode
    assert bool(src_mode & stat.S_IXUSR) == bool(out_mode & stat.S_IXUSR)
    assert bool(src_mode & stat.S_IXGRP) == bool(out_mode & stat.S_IXGRP)
    assert bool(src_mode & stat.S_IXOTH) == bool(out_mode & stat.S_IXOTH)


def test_s3_store_tar_excludes_directory_descendants(tmp_path):
    src = tmp_path / "src"
    (src / ".venv" / "bin").mkdir(parents=True)
    (src / "keep.txt").write_text("keep")
    (src / ".venv" / "bin" / "python").write_text("skip")

    store = S3Store()
    tar_uri = store.tar(src, excludes=[".venv"])
    out = tmp_path / "out"
    store.untar(tar_uri, out)

    assert (out / "keep.txt").read_text() == "keep"
    assert not (out / ".venv").exists()


def test_s3_store_tar_skips_absolute_symlinks_under_excluded_directory(tmp_path):
    src = tmp_path / "src"
    (src / ".venv" / "bin").mkdir(parents=True)
    (src / "keep.txt").write_text("keep")
    (src / ".venv" / "bin" / "python").symlink_to(Path(sys.executable))

    store = S3Store()
    tar_uri = store.tar(src, excludes=[".venv"])
    out = tmp_path / "out"
    store.untar(tar_uri, out)

    assert (out / "keep.txt").read_text() == "keep"
    assert not (out / ".venv").exists()


def test_s3_store_tar_raises_on_non_excluded_symlink_by_default(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "keep.txt").write_text("keep")
    (src / "link.txt").symlink_to(src / "keep.txt")

    store = S3Store()
    with pytest.raises(DmlRepoError, match="symlinks='raise'"):
        store.tar(src)


def test_s3_store_tar_ignores_non_excluded_symlink_when_requested(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "keep.txt").write_text("keep")
    (src / "link.txt").symlink_to(src / "keep.txt")

    store = S3Store()
    tar_uri = store.tar(src, symlinks="ignore")
    out = tmp_path / "out"
    store.untar(tar_uri, out)

    assert (out / "keep.txt").read_text() == "keep"
    assert not (out / "link.txt").exists()


def test_s3_store_cd_rebases_prefix():
    store = S3Store(bucket="test-bucket", prefix="a/b")
    next_store = store.cd("c")
    assert next_store.prefix.endswith("a/b/c")


def test_is_s3_uri_validation_matrix():
    assert is_s3_uri("s3://bucket/key") is True
    assert is_s3_uri("s3://bucket/dir/key.py") is True
    assert is_s3_uri("s3://bucket") is False
    assert is_s3_uri("https://bucket/key") is False
    assert is_s3_uri("") is False


def test_s3_store_requires_remote_uri_or_explicit_bucket(monkeypatch):
    monkeypatch.delenv("DML_REMOTE_URI", raising=False)
    with pytest.raises(DmlRepoError, match="requires configured remote.uri"):
        S3Store()


def test_s3_store_tar_is_reproducible_for_tests_assets():
    store = S3Store()
    assets_dir = Path(__file__).resolve().parents[1] / "assets"
    first = store.tar(assets_dir)
    second = store.tar(assets_dir)
    assert first.uri == second.uri


def test_s3_store_untar_rejects_path_traversal_by_default(tmp_path):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        payload = b"owned"
        info = tarfile.TarInfo(name="../escape.txt")
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))

    store = S3Store()
    tar_uri = store.put(data=buf.getvalue(), suffix=".tar")
    out = tmp_path / "out"
    with pytest.raises(DmlRepoError, match="outside destination"):
        store.untar(tar_uri, out)
    assert not (tmp_path / "escape.txt").exists()


def test_s3_store_untar_allows_unsafe_extract_when_explicit(tmp_path):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        payload = b"owned"
        info = tarfile.TarInfo(name="../escape.txt")
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))

    store = S3Store()
    tar_uri = store.put(data=buf.getvalue(), suffix=".tar")
    out = tmp_path / "out"
    store.untar(tar_uri, out, unsafe=True)
    assert (tmp_path / "escape.txt").read_bytes() == b"owned"
