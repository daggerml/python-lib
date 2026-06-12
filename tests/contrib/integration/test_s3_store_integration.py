from __future__ import annotations

import io
import tarfile

import pytest

from daggerml.api import DmlRepoError
from daggerml.contrib.s3 import S3Store


def test_contrib_int_003__moto_backed_s3store_roundtrip_succeeds(remote_env, s3_bucket):
    store = S3Store()
    uri = store.put(data=b"abc", suffix=".txt")
    assert store.get(uri) == b"abc"
    js = store.put_js({"a": 1})
    assert store.get_js(js) == {"a": 1}


def test_contrib_int_004__tar_safety_rejects_traversal_and_accepts_safe_archives(remote_env, s3_bucket, tmp_path):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        payload = b"owned"
        info = tarfile.TarInfo(name="../escape.txt")
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))
    store = S3Store()
    tar_uri = store.put(data=buf.getvalue(), suffix=".tar")
    with pytest.raises(DmlRepoError, match="outside destination"):
        store.untar(tar_uri, tmp_path / "out")

    safe_src = tmp_path / "src"
    safe_src.mkdir()
    (safe_src / "file.txt").write_text("ok")
    safe_uri = store.tar(safe_src)
    store.untar(safe_uri, tmp_path / "safe-out")
    assert (tmp_path / "safe-out" / "file.txt").read_text() == "ok"
