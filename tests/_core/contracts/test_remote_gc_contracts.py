from __future__ import annotations

import json
from types import SimpleNamespace

from daggerml._core.remote import Remote
from daggerml._core.types import Commit, Dag, DmlDB, LiteralNode, ScalarDatum, Tree


def make_db(path) -> DmlDB:
    path.mkdir()
    db = DmlDB(str(path), 1024 * 1024, 1024 * 1024 * 64)
    with db.tx(create_if_missing=True):
        pass
    return db


def test_remote_gc_001__reachable_objects_include_nested_objects_for_liveness(tmp_path) -> None:
    db = make_db(tmp_path / "db")
    with db.tx() as txn:
        scalar = txn.put(ScalarDatum("done"))
        result = txn.put(LiteralNode(value=scalar))
        dag_ref = txn.put(Dag(nodes=[result], names={"result": result}, tags=[], result=result))
        tree_ref = txn.put(Tree(dags={"main": dag_ref}))
        commit_ref = txn.put(
            Commit(
                parents=[],
                tree=tree_ref,
                author="alice",
                message="snapshot",
                created="2026-01-01T00:00:00Z",
            )
        )

    remote = object.__new__(Remote)
    remote.n_workers = 1
    remote.prune_age_seconds = 24 * 3600
    remote._store = SimpleNamespace(_key_for=lambda key: key)

    objects, missing = remote._collect_local_objects(commit_ref, db)
    assert missing == set()
    remote._store = SimpleNamespace(
        _key_for=lambda key: key,
        _get=lambda key: objects[key],
        client=SimpleNamespace(exceptions=SimpleNamespace(NoSuchKey=KeyError)),
    )

    live_oids = remote._get_live_oids(commit_ref)

    assert commit_ref.id() in live_oids
    assert tree_ref.id() in live_oids
    assert dag_ref.id() in live_oids
    assert result.id() in live_oids
    assert scalar.id() in live_oids

    payload = json.loads(objects[remote._cas_key(commit_ref.id())])
    assert payload == [
        "dict",
        {
            "author": ["scalar", "alice"],
            "created": ["scalar", "2026-01-01T00:00:00Z"],
            "message": ["scalar", "snapshot"],
            "parents": ["list", []],
            "tree": ["ref", tree_ref.to],
        },
    ]
