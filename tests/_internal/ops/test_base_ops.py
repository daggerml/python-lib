"""Comprehensive tests for base_ops.py module with real database testing."""

from tempfile import TemporaryDirectory

import pytest
from hypothesis import given

from daggerml._internal._db import DmlDbEnv, Ref
from daggerml._internal.ops.base_ops import BaseOps, with_retry
from daggerml._internal.types import NAMESPACES, Deletable, ScalarDatum, Uri
from tests._internal.test__db import _gen_ref
from tests._internal.test_types import DmlRepoError, _dml_obj_strategy


class TestBaseOps:
    """Test BaseOps functionality."""

    @given(_dml_obj_strategy())
    def test_putget_roundtrip(self, temp_bo, obj):
        """Test successful private _get operation."""
        with temp_bo._tx(readonly=False) as ctx:
            ref = ctx.put(obj)
        with temp_bo._tx(readonly=True) as ctx:
            retrieved_obj = ctx.get(ref)
        assert retrieved_obj == obj
        with temp_bo._tx(readonly=False) as ctx:
            ctx.delete(ref)

    @given(_dml_obj_strategy())
    def test_delete(self, temp_bo, obj):
        """Test successful private _delete operation."""
        with temp_bo._tx(readonly=False) as ctx:
            ref = ctx.put(obj)
        with temp_bo._tx(readonly=False) as ctx:
            ctx.delete(ref)
        with temp_bo._tx(readonly=True) as ctx:
            with pytest.raises(DmlRepoError, match="Object not found:"):
                ctx.get(ref)

    @given(_dml_obj_strategy())
    def test_iter(self, temp_bo, obj):
        """Test successful private _get operation."""
        with temp_bo._tx(readonly=False) as ctx:
            ref = ctx.put(obj)
        with temp_bo._tx(readonly=True) as ctx:
            assert [ref] == list(ctx.iter(ref.ns()))
        temp_bo._db.clear_all()

    @given(_dml_obj_strategy())
    def test_exists(self, temp_bo, obj):
        """Test successful private _get operation."""
        with temp_bo._tx(readonly=False) as ctx:
            ref = ctx.put(obj)
        with temp_bo._tx(readonly=True) as ctx:
            assert ctx.exists(ref)
        with temp_bo._tx(readonly=False) as ctx:
            ctx.delete(ref)
        with temp_bo._tx(readonly=True) as ctx:
            assert not ctx.exists(ref)
        temp_bo._db.clear_all()

    def test_get_error(self, temp_bo):
        """Test private _get operation with error."""
        with pytest.raises(DmlRepoError, match="Object not found:"):
            with temp_bo._tx(readonly=True) as ctx:
                ctx.get(_gen_ref("head"))

    def test_with_retry_retries_whole_transaction_on_map_full(self):
        """Map-full should resize and retry the whole operation, not a single put."""

        class ResizeHarness(BaseOps):
            def __post_init__(self):
                super().__post_init__()
                self.attempts = 0

            @with_retry
            def write_pair(self):
                self.attempts += 1
                with self._tx(readonly=False) as ctx:
                    first = ctx.put(ScalarDatum(data="first"))
                    second = ctx.put(ScalarDatum(data="x" * 700_000))
                    return first, second

        with TemporaryDirectory() as temp_dir:
            db = DmlDbEnv.create(temp_dir, namespaces=sorted(NAMESPACES), map_size=256 * 1024)
            ops = ResizeHarness(db)
            first_ref, second_ref = ops.write_pair()
            assert ops.attempts >= 2
            with ops._tx(readonly=True) as ctx:
                first_obj = ctx.get(first_ref)
                second_obj = ctx.get(second_ref)
                assert first_obj.data == "first"
                assert len(second_obj.data) == 700_000

    def test_uri_and_deletable_are_mutually_exclusive(self, temp_bo):
        uri = Uri(uri="s3://bucket/key")
        deletable = Deletable(uri=uri.uri)

        with temp_bo._tx(readonly=False) as ctx:
            uri_ref = ctx.put(uri)
            assert uri_ref.ns() == "datum-uri"
            assert ctx.exists(uri_ref)
            assert not ctx.exists(Ref(f"deletable:{uri_ref.id()}"))

            deletable_ref = ctx.put(deletable)
            assert deletable_ref.ns() == "deletable"
            assert ctx.exists(deletable_ref)
            assert not ctx.exists(uri_ref)

            uri_ref_2 = ctx.put(uri)
            assert uri_ref_2.id() == uri_ref.id()
            assert ctx.exists(uri_ref_2)
            assert not ctx.exists(deletable_ref)
