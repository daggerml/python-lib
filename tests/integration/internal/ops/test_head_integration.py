"""Comprehensive tests for head.py module with real database integration."""

import pytest

from daggerml._internal.ops.head import HeadOps
from daggerml._internal.types import Commit, DmlRepoError, Tree

pytestmark = pytest.mark.slow


class TestHeadOps:
    """Test HeadOps functionality with mocks."""

    def test_create_and_delete_branch_head_roundtrip(self, temp_bo):
        """Test HeadOps initialization."""
        branch_name = "feature"
        existing_branch = "main"
        ops = HeadOps(temp_bo._db)
        with temp_bo._tx(readonly=False) as txn:
            tree_ref = txn.put(Tree(dags={}))
            cr0 = txn.put(Commit(parents=[], tree=tree_ref, author="test", message="base"))
        ops.create_branch(existing_branch, cr0)
        ref = ops.create_branch(branch_name, cr0)
        assert ref == branch_name
        assert ops.get_branch_commit(ref) == cr0
        ops.delete_branch(ref)
        ops.delete_branch(existing_branch)
        with pytest.raises(DmlRepoError, match="Pointer does not exist"):
            ops.get_branch_commit(ref)
        with temp_bo._tx(readonly=False) as txn:
            txn.delete(cr0)

    def test_list(self, temp_bo):
        """Test HeadOps list method."""
        ops = HeadOps(temp_bo._db)
        with temp_bo._tx(readonly=False) as txn:
            tree_ref = txn.put(Tree(dags={}))
            commit_ref = txn.put(Commit(parents=[], tree=tree_ref, author="test", message="base"))
        head_names = ["main", "feature", "release_1"]
        for head_name in head_names:
            ops.create_branch(head_name, commit_ref)
        listed_heads = ops.list_branches()
        assert set(head_names).issubset(set(listed_heads))
        for head_name in head_names:
            ops.delete_branch(head_name)

    def test_get_branch_commit(self, temp_bo):
        with temp_bo._tx(readonly=False) as txn:
            tree_ref = txn.put(Tree(dags={}))
            commit_ref = txn.put(Commit(parents=[], tree=tree_ref, author="test", message="base"))
        ops = HeadOps(temp_bo._db)
        ops.create_branch("main", commit_ref)
        assert ops.get_branch_commit("main") == commit_ref
        ops.delete_branch("main")
