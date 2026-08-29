from __future__ import annotations

import io
import tarfile

import pytest

from daggerml import Uri
from daggerml.api import DmlRepoError
from daggerml.contrib.s3 import S3Store


def test_contrib_s3_001__remote_root_and_explicit_bucket_prefix_normalize(remote_env, s3_bucket):
    store = S3Store()
    uri = store.put(data=b"hello")
    assert isinstance(uri, Uri)
    assert uri.uri.startswith("s3://test-bucket/test-prefix/data/")
    assert S3Store(bucket="test-bucket", prefix="base").parse_uri("x") == ("test-bucket", "base/x")


def test_contrib_s3_002__put_get_exists_and_rm_are_content_addressed_helpers(remote_env, s3_bucket):
    store = S3Store()
    uri = store.put(data=b"abc", suffix=".txt")
    assert store.exists(uri) is True
    assert store.get(uri) == b"abc"
    store.rm(uri)
    assert store.exists(uri) is False


def test_contrib_s3_003__json_helpers_are_stable_and_json_safe(remote_env, s3_bucket):
    store = S3Store()
    uri = store.put_js({"b": 2, "a": 1})
    assert uri.uri.endswith(".json")
    assert store.get_js(uri) == {"a": 1, "b": 2}


def test_contrib_s3_004__tar_excludes_patterns_and_normalizes_metadata(remote_env, s3_bucket, tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "a.txt").write_text("A")
    (src / "b.tmp").write_text("TMP")
    tar_uri = S3Store().tar(src, excludes=["*.tmp"])
    out = tmp_path / "out"
    S3Store().untar(tar_uri, out)
    assert (out / "a.txt").read_text() == "A"
    assert not (out / "b.tmp").exists()


def test_contrib_s3_005__untar_rejects_unsafe_paths_by_default(remote_env, s3_bucket, tmp_path):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        payload = b"owned"
        info = tarfile.TarInfo(name="../escape.txt")
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))
    tar_uri = S3Store().put(data=buf.getvalue(), suffix=".tar")
    with pytest.raises(DmlRepoError, match="outside destination"):
        S3Store().untar(tar_uri, tmp_path / "out")
