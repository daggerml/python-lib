from daggerml._core.commit import CommitOps
from daggerml._core.types import Commit, Tree
from tests._core.helpers import make_db


def test_is_ancestor_traverses_all_merge_parents(tmp_path) -> None:
    db_path = tmp_path / "db"
    db_path.mkdir()
    db = make_db(db_path)
    with db.tx(create_if_missing=True) as txn:
        tree = txn.put(Tree(dags={}, tags={}))
        base = txn.put(Commit(tree=tree, parents=[], author="alice", message="base"))
        left = txn.put(Commit(tree=tree, parents=[base], author="alice", message="left"))
        right = txn.put(Commit(tree=tree, parents=[base], author="alice", message="right"))
        merged = txn.put(Commit(tree=tree, parents=[left, right], author="alice", message="merge"))

    ops = CommitOps()
    assert ops.is_ancestor(base, merged, db=db)
    assert ops.is_ancestor(left, merged, db=db)
    assert ops.is_ancestor(right, merged, db=db)
    assert not ops.is_ancestor(merged, left, db=db)
