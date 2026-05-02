"""Comprehensive tests for head.py module with real database integration."""

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from daggerml._internal.ops.head import HeadOps
from daggerml._internal.types import Head
from tests.contracts.internal.test_types_contract import _commit_strategy, _head_strategy, _refs

pytestmark = pytest.mark.slow


class TestHeadOps:
    """Test HeadOps functionality with mocks."""

    @given(_commit_strategy(), _refs("head", full=True), _refs("head", full=True))
    def test_create_and_delete_branch_head_roundtrip(self, temp_bo, c0, hr0, br_ref):
        """Test HeadOps initialization."""
        branch_name = br_ref.id()
        assume(branch_name != hr0.id())
        # assume(branch_name.isascii())
        with temp_bo._tx(readonly=False) as txn:
            cr0 = txn.put(c0)
            txn.put(Head(commit=cr0), to=hr0)
        ops = HeadOps(temp_bo._db)
        ref = ops.create_branch(branch_name, cr0)
        assert ref == branch_name
        with temp_bo._tx(readonly=True) as txn:
            assert txn.get(ops._branch_ref(ref)) == Head(commit=cr0)
        ops.delete_branch(ref)
        with temp_bo._tx(readonly=True) as txn:
            assert not txn.exists(ops._branch_ref(ref))
        with temp_bo._tx(readonly=False) as txn:
            txn.delete(hr0)
            txn.delete(cr0)

    @given(
        st.dictionaries(
            _refs("head", full=True),
            _head_strategy(),
            min_size=1,
            max_size=1,
        )
    )
    def test_list(self, temp_bo, head_dict):
        """Test HeadOps list method."""
        ops = HeadOps(temp_bo._db)
        with temp_bo._tx(readonly=False) as txn:
            head_refs = [txn.put(v, to=k) for k, v in head_dict.items()]
        listed_heads = ops.list_branches()
        assert set(listed_heads) == {ref.id() for ref in head_refs}
        assert len(listed_heads) == len(head_dict)
        with temp_bo._tx(readonly=False) as txn:
            for hr in head_refs:
                txn.delete(hr)

    @given(_commit_strategy(), _refs("head", full=True))
    def test_get_branch_commit(self, temp_bo, commit_obj, head_ref):
        with temp_bo._tx(readonly=False) as txn:
            commit_ref = txn.put(commit_obj)
            txn.put(Head(commit=commit_ref), to=head_ref)
        ops = HeadOps(temp_bo._db)
        assert ops.get_branch_commit(head_ref.id()) == commit_ref
