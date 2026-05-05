"""Comprehensive tests for head.py module with real database integration."""

from pathlib import Path

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

    def test_bootstrap_branch_pointer_is_written_after_commit_is_visible(self, temp_bo, monkeypatch):
        ops = HeadOps(temp_bo._db)
        seen = {}
        create_pointer = ops._create_pointer

        def _spy(pointer_path, commit_ref):
            with temp_bo._tx(readonly=True) as txn:
                seen["exists"] = txn.exists(commit_ref)
            return create_pointer(pointer_path, commit_ref)

        monkeypatch.setattr(ops, "_create_pointer", _spy)

        branch = ops.create_branch("main")

        assert branch == "main"
        assert seen["exists"] is True
        assert ops.get_branch_commit(branch).ns() == "commit"

    def test_bootstrap_branch_with_caller_txn_is_rejected(self, temp_bo):
        ops = HeadOps(temp_bo._db)

        with temp_bo._tx(readonly=False) as txn:
            with pytest.raises(DmlRepoError, match="does not support caller-owned transactions"):
                ops.create_branch("txn-main", txn=txn)

    def test_index_pointer_ops_do_not_require_live_commits(self, temp_bo):
        ops = HeadOps(temp_bo._db)
        missing_commit = ops.get_branch_commit(ops.create_branch("stale-main"))
        with temp_bo._tx(readonly=False) as txn:
            txn.delete(missing_commit)

        index_id = ops.create_index(missing_commit)
        updated_commit = type(missing_commit)(f"commit:{'f' * 64}")

        assert ops.get_index_commit(index_id) == missing_commit
        assert ops.update_index_commit(index_id, missing_commit, updated_commit) == updated_commit
        assert ops.get_index_commit(index_id) == updated_commit

        ops.delete_index(index_id)
        with pytest.raises(DmlRepoError, match="Pointer does not exist"):
            ops.get_index_commit(index_id)

    def test_head_roundtrip_supports_attached_and_detached_payloads(self, temp_bo):
        ops = HeadOps(temp_bo._db)
        branch = ops.create_branch("feature")
        branch_commit = ops.get_branch_commit(branch)

        ops.write_attached_head("feature")
        attached = ops.get_head_state()
        assert attached.mode == "attached"
        assert attached.branch == "feature"
        assert attached.commit == branch_commit

        ops.write_detached_head(branch_commit)
        detached = ops.get_head_state()
        assert detached.mode == "detached"
        assert detached.branch is None
        assert detached.commit == branch_commit

    def test_invalid_head_payload_fails_closed(self, temp_bo):
        ops = HeadOps(temp_bo._db)
        head_path = ops._head_path()
        head_path.parent.mkdir(parents=True, exist_ok=True)
        head_path.write_text("main\n")

        with pytest.raises(DmlRepoError, match="Invalid HEAD payload"):
            ops.get_head_state()

    def test_attached_head_accepts_slash_and_dot_branch_names(self, temp_bo):
        ops = HeadOps(temp_bo._db)
        branch = ops.create_branch("topic/v1.0")
        ops.write_attached_head(branch)

        state = ops.get_head_state()

        assert state.mode == "attached"
        assert state.branch == "topic/v1.0"
        assert "topic/v1.0" in ops.list_branches()

    def test_project_home_requires_dml_db_layout(self, temp_bo):
        ops = HeadOps(temp_bo._db)
        original_path = ops._db.path
        ops._db.path = str(Path(temp_bo._db.path).parent.parent)
        try:
            with pytest.raises(DmlRepoError, match="Cannot resolve project home"):
                ops.get_head_state()
        finally:
            ops._db.path = original_path
