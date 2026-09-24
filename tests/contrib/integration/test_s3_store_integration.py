from __future__ import annotations

import io
import tarfile

import pytest

import daggerml.api as api
from daggerml import Dml
from daggerml.api import DmlRepoError
from daggerml.contrib.s3 import S3Store


def test_moto_backed_s3store_roundtrip_succeeds(remote_env, s3_bucket):
    store = S3Store()
    uri = store.put(data=b"abc", suffix=".txt")
    assert store.get(uri) == b"abc"
    js = store.put_js({"a": 1})
    assert store.get_js(js) == {"a": 1}


def test_moto_artifact_survives_project_publication_and_clone(remote_world, monkeypatch):
    world = remote_world("artifact-roundtrip")
    with api.new("seed", dml=world.publisher) as dag:
        dag.commit(dag.put("seed"))
    monkeypatch.setenv("DML_REMOTE_ROOT", world.root)
    store = S3Store()
    uri = store.put(data=b"artifact-roundtrip", suffix=".txt")
    with api.new("artifact", dml=world.publisher) as dag:
        dag.commit(dag.put(uri))
    world.publisher.push()
    clone = Dml.clone("main", project_home=str(world.home / "clone"), remote_root=world.root)
    assert store.get(api.load("artifact", dml=clone).result.value()) == b"artifact-roundtrip"


def test_tar_safety_rejects_traversal_and_accepts_safe_archives(remote_env, s3_bucket, tmp_path):
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
