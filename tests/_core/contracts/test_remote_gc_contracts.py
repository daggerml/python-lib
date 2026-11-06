"""GC marks manifest dependencies without materializing or inspecting graphs."""

import json
from collections import Counter
from types import SimpleNamespace

import pytest

from daggerml._core.db import Ref
from daggerml._core.remote import Remote, _encode_cas_value
from daggerml._core.types import DmlRepoError


def test_marking_reads_shared_history_and_import_manifests_once_without_reading_packs():
    remote = object.__new__(Remote)
    remote._store = SimpleNamespace(_key_for=lambda key: key)
    parent, tip = [Ref(f"commit:{i:064x}") for i in range(2)]
    dag, imported, function = [Ref(f"dag:{i:064x}") for i in range(3)]
    argv = Ref(f"node-argv:{0:064x}")
    manifests = {
        remote._commit_key(tip): {"commit": [tip.to, json.dumps(_encode_cas_value({"parents": [parent]}))],
                                  "dags": {"main": dag.to, "alias": dag.to}},
        remote._commit_key(parent): {"commit": [parent.to, json.dumps(_encode_cas_value({"parents": []}))],
                                     "dags": {"previous": imported.to}},
        remote._manifest_key(dag): {"subdags": [function.to], "imports": [imported.to]},
        remote._manifest_key(imported): {"subdags": [function.to], "imports": []},
        remote._manifest_key(argv): {"subdags": [], "imports": []},
    }
    reads = Counter()

    def get(key):
        reads[key] += 1
        return json.dumps(manifests[key])

    remote._store._get = get
    live = remote._get_live_keys({tip, parent, dag, imported, argv})
    assert live == set(manifests) | {remote._pack_key(ref) for ref in (dag, imported, function, argv)}
    assert reads == Counter(dict.fromkeys(manifests, 1))


def test_batch_deletion_splits_requests_and_deletes_each_key_once():
    remote = object.__new__(Remote)
    deleted, batches = [], []

    def delete_objects(*, Bucket, Delete):
        batch = [obj["Key"] for obj in Delete["Objects"]]
        batches.append(batch)
        deleted.extend(batch)
        return {}

    remote._store = SimpleNamespace(bucket="bucket", client=SimpleNamespace(delete_objects=delete_objects))
    keys = [f"key-{i}" for i in range(2005)]
    remote._delete_objects(keys)
    assert deleted == keys
    assert len(batches) == 3
    assert all(0 < len(batch) <= 1000 for batch in batches)


def test_batch_deletion_reports_s3_partial_failure_with_uri():
    remote = object.__new__(Remote)
    remote._store = SimpleNamespace(bucket="bucket", client=SimpleNamespace(
        delete_objects=lambda **kwargs: {"Errors": [{"Key": "blocked", "Code": "AccessDenied"}]},
    ))
    with pytest.raises(DmlRepoError, match="s3://bucket/blocked.*AccessDenied"):
        remote._delete_objects(["deleted", "blocked"])
