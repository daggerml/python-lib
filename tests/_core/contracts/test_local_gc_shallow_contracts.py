from __future__ import annotations

import pytest

from daggerml._core.db import DmlDbKeyNotFoundError, Ref
from daggerml._core.head import Head
from daggerml._core.types import Commit, ScalarDatum, Tree
from tests._core.helpers import make_db, make_local_dml


def _put_shallow_history(db):
    missing = Ref("commit:" + "a" * 64)

    def write(txn):
        tree = txn.put(Tree(dags={}))
        root = txn.put(Commit(parents=[missing], tree=tree, author="user", message="shallow"))
        orphan = txn.put(ScalarDatum("orphan"))
        return root, tree, orphan

    root, tree, orphan = db.write_with_growth(write)
    return root, tree, orphan, missing


def test_gc_accepts_only_declared_missing_commit_leaves(tmp_path) -> None:
    db = make_db(tmp_path)
    root, tree, orphan, missing = _put_shallow_history(db)

    assert db.gc([root], {missing}) == {"datum-scalar": 1}

    with db.tx(readonly=True) as txn:
        assert txn.exists(root)
        assert txn.exists(tree)
        assert not txn.exists(orphan)


def test_gc_rejects_undeclared_missing_commit_without_deleting(tmp_path) -> None:
    db = make_db(tmp_path)
    root, _tree, orphan, _missing = _put_shallow_history(db)

    with pytest.raises(DmlDbKeyNotFoundError):
        db.gc([root])

    with db.tx(readonly=True) as txn:
        assert txn.exists(orphan)


def test_gc_rejects_declared_missing_commit_as_root(tmp_path) -> None:
    db = make_db(tmp_path)
    db.init()
    missing = Ref("commit:" + "b" * 64)

    with pytest.raises(DmlDbKeyNotFoundError):
        db.gc([missing], {missing})


def test_gc_rejects_missing_commit_as_root_even_after_reaching_it_as_parent(tmp_path) -> None:
    db = make_db(tmp_path)
    root, _tree, _orphan, missing = _put_shallow_history(db)

    with pytest.raises(DmlDbKeyNotFoundError):
        db.gc([root, missing], {missing})


def test_gc_rejects_non_commit_missing_leaf_declaration(tmp_path) -> None:
    db = make_db(tmp_path)
    root = db.init()

    with pytest.raises(TypeError, match="expected namespace hierarchy"):
        db.gc([root], {Ref("tree:" + "c" * 64)})


def test_dml_gc_reads_shallow_metadata(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    root, _tree, _orphan, missing = _put_shallow_history(dml._db)
    head = Head(str(tmp_path))
    head.update_local_ref("main", root)
    head.write_shallow_commits({missing})

    result = dml.gc()

    assert result["deleted"]["datum-scalar"] == 1


def test_dml_gc_removes_stale_shallow_entries(tmp_path, monkeypatch) -> None:
    dml = make_local_dml(tmp_path, monkeypatch)
    root, _tree, _orphan, missing = _put_shallow_history(dml._db)
    stale = Ref("commit:" + "d" * 64)
    head = Head(str(tmp_path))
    head.update_local_ref("main", root)
    head.write_shallow_commits({missing, stale})

    dml.gc()

    assert head.get_shallow_commits() == {missing}
