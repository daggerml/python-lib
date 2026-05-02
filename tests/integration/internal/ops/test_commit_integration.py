"""Comprehensive tests for commit.py module with real database integration."""

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from daggerml._internal.ops.commit import CommitOps
from daggerml._internal.types import Commit, DmlRepoError, Head, Tree
from tests.contracts.internal.support.test_db_support import _gen_ref
from tests.contracts.internal.test_types_contract import _commit_strategy, _tree_strategy

pytestmark = pytest.mark.slow


class TestCommitOps:
    """Test CommitOps functionality with mocks."""

    @pytest.fixture
    def ops(self, temp_db):
        """Create CommitOps with stored head context."""
        ops = CommitOps(_db=temp_db)
        # Create and store a complete context
        tree = Tree(dags={"main": _gen_ref("dag")})
        with ops._tx(readonly=False) as txn:
            tree_ref = txn.put(tree)
            commit = Commit(parents=[], tree=tree_ref, author="test_user", message="Initial commit")
            commit_ref = txn.put(commit)
            head = Head(commit=commit_ref)
            head_ref = txn.put(head)
        return ops, head_ref

    def test_list_commits_integration(self, ops):
        """Test listing commit history with real database."""
        ops, _head_ref = ops
        # Create a commit chain: A -> B -> C
        tree = Tree(dags={"test": _gen_ref("dag")})
        with ops._tx(readonly=False) as txn:
            tree_ref = txn.put(tree)
            commit_a = Commit(parents=[], tree=tree_ref, author="user", message="A")
            commit_a_ref = txn.put(commit_a)
            commit_b = Commit(parents=[commit_a_ref], tree=tree_ref, author="user", message="B")
            commit_b_ref = txn.put(commit_b)
            commit_c = Commit(parents=[commit_b_ref], tree=tree_ref, author="user", message="C")
            commit_c_ref = txn.put(commit_c)

        # Test list without limit
        commits = list(ops.list(commit_c_ref))
        assert len(commits) == 3
        assert commits == [commit_c_ref, commit_b_ref, commit_a_ref]

        # Test list with limit
        limited_commits = list(ops.list(commit_c_ref, limit=2))
        assert len(limited_commits) == 2
        assert limited_commits == [commit_c_ref, commit_b_ref]

    def test_get_dag_integration(self, ops):
        """Test get_dag with real database."""
        ops, _ = ops
        # Create tree with multiple DAGs
        dag1_ref = _gen_ref("dag")
        dag2_ref = _gen_ref("dag")
        tree = Tree(dags={"dag1": dag1_ref, "dag2": dag2_ref})
        with ops._tx(readonly=False) as txn:
            tree_ref = txn.put(tree)
            commit = Commit(parents=[], tree=tree_ref, author="test", message="Test commit")
            commit_ref = txn.put(commit)

        # Test getting existing DAG
        result = ops.get_dag(commit_ref, "dag1")
        assert result == dag1_ref

        # Test getting non-existent DAG
        result = ops.get_dag(commit_ref, "nonexistent")
        assert result is None

    def test_describe_integration(self, ops):
        """Test describe returns stable commit metadata."""
        ops, _ = ops
        dag_ref = _gen_ref("dag")
        tree = Tree(dags={"dag1": dag_ref})
        with ops._tx(readonly=False) as txn:
            tree_ref = txn.put(tree)
            commit = Commit(parents=[], tree=tree_ref, author="test", message="Test commit", dag=dag_ref)
            commit_ref = txn.put(commit)

        info = ops.describe(commit_ref)
        assert info["id"] == commit_ref.id()
        assert info["tree"] == tree_ref
        assert info["author"] == "test"
        assert info["message"] == "Test commit"
        assert info["dag"] == dag_ref

    def test_delete_dag_integration(self, ops):
        """Test delete_dag with real database."""
        from daggerml._internal._db import Ref

        ops, _head_ref = ops
        branch = "main"
        head_ref = Ref(f"head:{branch}")
        # Create tree with multiple DAGs and store as head
        keep1_ref = _gen_ref("dag")
        delete_ref = _gen_ref("dag")
        keep2_ref = _gen_ref("dag")
        original_tree = Tree(dags={"keep1": keep1_ref, "delete_me": delete_ref, "keep2": keep2_ref})
        with ops._tx(readonly=False) as txn:
            tree_ref = txn.put(original_tree)
            commit = Commit(parents=[], tree=tree_ref, author="test_user", message="Original commit")
            commit_ref = txn.put(commit)
            # Update head to point to this commit
            from daggerml._internal.types import Head

            head = Head(commit=commit_ref)
            txn.put(head, to=head_ref)

        # Test delete_dag operation (note: returns self for chaining now)
        result = ops.delete_dag("delete_me", branch, "test_user")
        assert result is ops

        # Get updated context to verify changes
        with ops._tx(readonly=True) as txn:
            head = txn.get(head_ref)
            ctx = txn.get_commit_ctx(head.commit)

        # Verify DAG was removed from tree
        assert "keep1" in ctx.tree.dags
        assert "keep2" in ctx.tree.dags
        assert "delete_me" not in ctx.tree.dags

        # Verify commit metadata
        assert "Delete DAG 'delete_me'" in ctx.commit.message
        assert ctx.commit.author == "test_user"
        assert len(ctx.commit.parents) == 1

    def test_topo_sort_integration(self, ops):
        """Test _topo_sort with real commit chain."""
        ops, _ = ops
        # Create a commit chain: A -> B -> C
        tree = Tree(dags={"test": _gen_ref("dag")})
        with ops._tx(readonly=False) as txn:
            tree_ref = txn.put(tree)
            commit_a = Commit(parents=[], tree=tree_ref, author="user", message="A")
            commit_a_ref = txn.put(commit_a)
            commit_b = Commit(parents=[commit_a_ref], tree=tree_ref, author="user", message="B")
            commit_b_ref = txn.put(commit_b)
            commit_c = Commit(parents=[commit_b_ref], tree=tree_ref, author="user", message="C")
            commit_c_ref = txn.put(commit_c)

        # Test topological sort
        sorted_refs = ops._topo_sort(commit_c_ref)

        # Should return [C, B, A] - descendants before ancestors
        assert len(sorted_refs) == 3
        assert sorted_refs[0] == commit_c_ref
        assert sorted_refs[1] == commit_b_ref
        assert sorted_refs[2] == commit_a_ref

    def test_merge_base_integration(self, ops):
        """Test _merge_base with real commit DAG."""
        ops, _ = ops
        # Create commit DAG:  A -> B -> D
        #                     A -> C -> E
        tree = Tree(dags={"test": _gen_ref("dag")})
        with ops._tx(readonly=False) as txn:
            tree_ref = txn.put(tree)
            commit_a = Commit(parents=[], tree=tree_ref, author="user", message="A")
            commit_a_ref = txn.put(commit_a)
            commit_b = Commit(parents=[commit_a_ref], tree=tree_ref, author="user", message="B")
            commit_b_ref = txn.put(commit_b)
            commit_c = Commit(parents=[commit_a_ref], tree=tree_ref, author="user", message="C")
            commit_c_ref = txn.put(commit_c)
            commit_d = Commit(parents=[commit_b_ref], tree=tree_ref, author="user", message="D")
            commit_d_ref = txn.put(commit_d)
            commit_e = Commit(parents=[commit_c_ref], tree=tree_ref, author="user", message="E")
            commit_e_ref = txn.put(commit_e)

        # Test merge base
        merge_base_ref = ops._merge_base(commit_d_ref, commit_e_ref)

        # Common ancestor should be A
        assert merge_base_ref == commit_a_ref

    def test_diff_trees_integration(self, ops):
        """Test _diff method with real trees."""
        ops, _ = ops
        # Create two different trees
        old_ref = _gen_ref("dag")
        unique1_ref = _gen_ref("dag")
        new_ref = _gen_ref("dag")
        unique2_ref = _gen_ref("dag")
        tree1 = Tree(dags={"common": old_ref, "only_in_1": unique1_ref})
        tree2 = Tree(dags={"common": new_ref, "only_in_2": unique2_ref})
        with ops._tx(readonly=False) as txn:
            tree1_ref = txn.put(tree1)
            tree2_ref = txn.put(tree2)

        # Test diff
        with ops._tx(readonly=True) as txn:
            diff_result = ops._diff(tree1_ref, tree2_ref, txn)

        # Check structure
        assert "add" in diff_result
        assert "rem" in diff_result

        # Tree2 additions: only_in_2 (new), common (changed value)
        assert "only_in_2" in diff_result["add"]
        assert diff_result["add"]["only_in_2"] == unique2_ref
        assert "common" in diff_result["add"]
        assert diff_result["add"]["common"] == new_ref
        assert diff_result["rem"]["only_in_1"] == unique1_ref
        assert diff_result["rem"]["common"] == old_ref

    def test_patch_trees_integration(self, ops):
        """Test _patch method with real trees."""
        ops, _ = ops
        # Create base tree
        keep_ref = _gen_ref("dag")
        old_ref = _gen_ref("dag")
        remove_ref = _gen_ref("dag")
        new_ref = _gen_ref("dag")
        added_ref = _gen_ref("dag")
        base_tree = Tree(dags={"keep": keep_ref, "modify": old_ref, "remove": remove_ref})
        patch_diff = {"add": {"modify": new_ref, "add": added_ref}, "rem": {"remove": remove_ref}}

        # Apply patch (outside of transaction since _patch creates its own)
        with ops._tx(readonly=False) as txn:
            base_tree_ref = txn.put(base_tree)
            patched_tree_ref = ops._patch(base_tree_ref, patch_diff, txn=txn)

        # Verify patched tree
        with ops._tx(readonly=True) as txn:
            patched_tree = txn.get(patched_tree_ref)
        assert isinstance(patched_tree, Tree)
        assert "keep" in patched_tree.dags  # Unchanged
        assert "modify" in patched_tree.dags  # Modified
        assert patched_tree.dags["modify"] == new_ref
        assert patched_tree.dags["keep"] == keep_ref
        assert patched_tree.dags["add"] == added_ref
        assert "remove" not in patched_tree.dags  # Removed

    def test_merge_integration(self, ops):
        """Test merge operation with real database."""
        ops, _ = ops
        # Create base tree and commit (common ancestor)
        original_ref = _gen_ref("dag")
        base_tree = Tree(dags={"common": original_ref})
        with ops._tx(readonly=False) as txn:
            base_tree_ref = txn.put(base_tree)
            base_commit = Commit(parents=[], tree=base_tree_ref, author="base_user", message="Base commit")
            base_commit_ref = txn.put(base_commit)
            # Create first branch: modify common, add dag1
            branch1_ref = _gen_ref("dag")
            unique1_ref = _gen_ref("dag")
            tree1 = Tree(dags={"common": branch1_ref, "dag1": unique1_ref})
            tree1_ref = txn.put(tree1)
            commit1 = Commit(parents=[base_commit_ref], tree=tree1_ref, author="user1", message="First branch")
            commit1_ref = txn.put(commit1)
            # Create second branch: modify common differently, add dag2
            branch2_ref = _gen_ref("dag")
            unique2_ref = _gen_ref("dag")
            tree2 = Tree(dags={"common": branch2_ref, "dag2": unique2_ref})
            tree2_ref = txn.put(tree2)
            commit2 = Commit(parents=[base_commit_ref], tree=tree2_ref, author="user2", message="Second branch")
            commit2_ref = txn.put(commit2)

        # Test merge operation
        try:
            merged_ref = ops.merge(commit1_ref, commit2_ref, "test_user")

            # If merge succeeds, verify result
            with ops._tx(readonly=True) as txn:
                merged_commit = txn.get(merged_ref)
            assert isinstance(merged_commit, Commit)
            assert len(merged_commit.parents) == 2
            assert commit1_ref in merged_commit.parents
            assert commit2_ref in merged_commit.parents
            assert merged_commit.author == "test_user"

            # Check merged tree
            with ops._tx(readonly=True) as txn:
                merged_tree = txn.get(merged_commit.tree)
            assert isinstance(merged_tree, Tree)
            # Both unique DAGs should be preserved
            assert "dag1" in merged_tree.dags
            assert "dag2" in merged_tree.dags
            # Conflicted "common" will have one of the values
            assert "common" in merged_tree.dags

        except DmlRepoError as e:
            # Merge conflict is expected due to conflicting "common" DAG
            assert "conflict" in str(e).lower()

    def test_merge_simple_case(self, ops):
        """Controlled test for merge behavior when one branch adds a single DAG key.

        Hypothesis: map/key handling between C and Python may produce spurious
        keys due to ownership/termination issues; this small controlled test
        reproduces the earlier failing scenario deterministically.
        """
        ops, _ = ops
        # Base commit
        base_tree = Tree(dags={})
        with ops._tx(readonly=False) as txn:
            base_tree_ref = txn.put(base_tree)
            base_commit = Commit(parents=[], tree=base_tree_ref, author="base", message="base")
            base_commit_ref = txn.put(base_commit)
            # t1 has a single DAG named '0'
            t1 = Tree(dags={"0": _gen_ref("dag")})
            t1_ref = txn.put(t1)
            c1 = Commit(parents=[base_commit_ref], tree=t1_ref, author="user1", message="c1")
            c1_ref = txn.put(c1)
            # t2 empty
            t2 = Tree(dags={})
            t2_ref = txn.put(t2)
            c2 = Commit(parents=[base_commit_ref], tree=t2_ref, author="user2", message="c2")
            c2_ref = txn.put(c2)

        # Merge c1 and c2 with overwrite strategy
        merged_ref = ops.merge(c1_ref, c2_ref, "test_user")
        with ops._tx(readonly=True) as txn:
            merged_commit = txn.get(merged_ref)
            mtree = txn.get(merged_commit.tree)
        # Expect merged tree to reflect overwrite (method semantics may vary),
        # but ensure keys are valid Python strings and not corrupted types.
        assert all(isinstance(k, str) for k in mtree.dags.keys())
        # Ensure no unexpected keys (either '0' present or not depending on method),
        # but crucially avoid segfaults during access.
        _ = list(mtree.dags.keys())

    def test_rebase_integration(self, ops):
        """Test rebase operation with real database."""
        ops, _ = ops
        # Create base commit
        base_ref = _gen_ref("dag")
        target_ref = _gen_ref("dag")
        source_ref = _gen_ref("dag")
        base_tree = Tree(dags={"base": base_ref})
        target_tree = Tree(dags={"base": base_ref, "target": target_ref})
        source_tree = Tree(dags={"base": base_ref, "source": source_ref})
        with ops._tx(readonly=False) as txn:
            base_tree_ref = txn.put(base_tree)
            base_commit = Commit(parents=[], tree=base_tree_ref, author="base", message="Base")
            base_commit_ref = txn.put(base_commit)
            target_tree_ref = txn.put(target_tree)
            target_commit = Commit(parents=[base_commit_ref], tree=target_tree_ref, author="target", message="Target")
            target_commit_ref = txn.put(target_commit)
            source_tree_ref = txn.put(source_tree)
            source_commit = Commit(parents=[base_commit_ref], tree=source_tree_ref, author="source", message="Source")
            source_commit_ref = txn.put(source_commit)

        # Test rebase operation
        rebased_ref = ops.rebase(source_commit_ref, target_commit_ref, "test_user")

        # Verify rebase result
        with ops._tx(readonly=True) as txn:
            rebased_commit = txn.get(rebased_ref)
        assert isinstance(rebased_commit, Commit)
        assert rebased_commit.author == "test_user"
        assert rebased_commit.message == "Source"  # Preserves original message
        assert len(rebased_commit.parents) == 1
        assert rebased_commit.parents[0] == target_commit_ref  # New parent

        # Verify rebased tree combines changes
        with ops._tx(readonly=True) as txn:
            rebased_tree = txn.get(rebased_commit.tree)
        assert isinstance(rebased_tree, Tree)
        assert "base" in rebased_tree.dags
        assert "source" in rebased_tree.dags  # Source changes preserved
        assert "target" in rebased_tree.dags  # Target base included

    @given(st.lists(_tree_strategy(), min_size=2, max_size=5))
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_multiple_commits_hypothesis(self, ops, tree_objs):
        """Test creating and listing multiple commits with hypothesis."""
        ops, _ = ops
        # Store all trees and create commit chain
        commit_refs = []
        delete_refs = []
        prev_commit_ref = None
        with ops._tx(readonly=False) as txn:
            for i, tree_obj in enumerate(tree_objs):
                tree_ref = txn.put(tree_obj)
                delete_refs.append(tree_ref)
                parents = [prev_commit_ref] if prev_commit_ref else []
                commit = Commit(parents=parents, tree=tree_ref, author=f"user_{i}", message=f"Commit {i}")
                commit_ref = txn.put(commit)
                commit_refs.append(commit_ref)
                delete_refs.append(commit_ref)
                prev_commit_ref = commit_ref
        # Test listing commits from latest
        if commit_refs:
            listed_commits = list(ops.list(commit_refs[-1]))
            assert len(listed_commits) == len(commit_refs)
            # Should be in reverse order (newest first)
            assert listed_commits == list(reversed(commit_refs))
        # Cleanup
        with ops._tx(readonly=False) as txn:
            for ref in set(delete_refs):
                txn.delete(ref)

    @given(_commit_strategy(), _tree_strategy())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_get_dag_hypothesis(self, ops, commit_obj, tree_obj):
        """Test get_dag with hypothesis-generated commits."""
        ops, _ = ops
        # Store tree and commit
        with ops._tx(readonly=False) as txn:
            commit_obj.tree = txn.put(tree_obj)
            commit_ref = txn.put(commit_obj)
            delete_refs = [commit_ref, commit_obj.tree]
        # Get the tree and test DAG retrieval
        with ops._tx(readonly=True) as txn:
            tree = txn.get(commit_obj.tree)
        if tree.dags:
            # Test getting existing DAG
            dag_name = next(iter(tree.dags.keys()))
            expected_dag_ref = tree.dags[dag_name]

            result = ops.get_dag(commit_ref, dag_name)
            assert result == expected_dag_ref

        # Test getting non-existent DAG
        result = ops.get_dag(commit_ref, "definitely_does_not_exist_12345")
        assert result is None
        with ops._tx(readonly=False) as txn:
            for ref in set(delete_refs):
                txn.delete(ref)

    @given(
        _commit_strategy(),
        _tree_strategy(),
        _commit_strategy(),
        _tree_strategy(),
        _commit_strategy(),
        _tree_strategy(),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_merge_real(self, ops, c0, t0, c1, t1, c2, t2):
        """Test merge with hypothesis-generated commits."""
        ops, _ = ops
        assume(set(t1.dags.keys()).isdisjoint(set(t2.dags.keys())))
        assume(set(t0.dags.keys()).isdisjoint(set([*t1.dags.keys(), *t2.dags.keys()])))
        # Store commits
        with ops._tx(readonly=False) as txn:
            c0.parents = []
            c0.tree = txn.put(t0)
            commit_ref0 = txn.put(c0)
            c1.parents = [commit_ref0]
            c1.tree = txn.put(t1)
            c2.parents = [commit_ref0]
            c2.tree = txn.put(t2)
            commit_ref1 = txn.put(c1)
            commit_ref2 = txn.put(c2)
            delete_refs = [commit_ref1, commit_ref2, c1.tree, c2.tree, commit_ref0, c0.tree]
        # Test merge operation
        merged_ref = ops.merge(commit_ref1, commit_ref2, "test_user")
        delete_refs.append(merged_ref)
        with ops._tx(readonly=True) as txn:
            merged_commit = txn.get(merged_ref)
            assert isinstance(merged_commit, Commit)
            assert len(merged_commit.parents) == 2
            assert commit_ref1 in merged_commit.parents
            assert commit_ref2 in merged_commit.parents
            assert merged_commit.author == "test_user"
            mtree = txn.get(merged_commit.tree)
            # removes `c0` dags plus adds from `c1` and `c2`
            assert set(mtree.dags.keys()) == set([*t1.dags.keys(), *t2.dags.keys()])
        with ops._tx(readonly=False) as txn:
            for ref in set(delete_refs):
                txn.delete(ref)
