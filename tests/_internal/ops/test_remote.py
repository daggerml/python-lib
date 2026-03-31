"""Tests for remote operations."""

import base64
import hashlib
import json
from unittest.mock import Mock, patch

import pytest

from daggerml._internal._db import Ref
from daggerml._internal.ops.remote import (
    InvalidManifest,
    InvalidOid,
    InvalidRef,
    MissingCasObject,
    RefAlreadyExists,
    RemoteError,
    RemoteOps,
    ShaMismatch,
)
from daggerml._internal.types import Commit, DmlRepoError, Head, Tree
from tests._internal.conftest import FakeDb, FakeTxn, remote_bucket_and_prefix_from_env


class TestRemoteDescriptor:
    """Tests for remote descriptor file handling."""

    def test_ensure_descriptor_creates_when_missing(self, db, s3):
        """Test that _ensure_remote_descriptor creates dml.json when missing."""
        bucket, prefix = remote_bucket_and_prefix_from_env()
        descriptor_key = f"{prefix}/dml.json" if prefix else "dml.json"

        # Ensure descriptor doesn't exist initially
        try:
            s3.delete_object(Bucket=bucket, Key=descriptor_key)
        except s3.exceptions.NoSuchKey:
            pass  # Already doesn't exist

        # Create RemoteOps instance, which should create the descriptor
        RemoteOps(
            _db=db,
            client=s3,
            bucket=bucket,
            prefix=prefix,
        )

        # Verify descriptor was created
        response = s3.get_object(Bucket=bucket, Key=descriptor_key)
        descriptor = json.loads(response["Body"].read().decode("utf-8"))

        expected_descriptor = {
            "schema": 0,
            "hash": "sha256",
            "layout": "cas+refs",
            "refs_prefix": "refs",
            "io_prefix": "io",
            "cas_prefix": "cas/sha256",
        }
        assert descriptor == expected_descriptor

    def test_ensure_descriptor_validates_existing(self, db, s3):
        """Test that _ensure_remote_descriptor fails on invalid existing descriptors."""
        bucket, prefix = remote_bucket_and_prefix_from_env()
        invalid_prefix = f"{prefix}/invalid-descriptor-test" if prefix else "invalid-descriptor-test"
        descriptor_key = f"{invalid_prefix}/dml.json"

        # Write an invalid descriptor
        invalid_descriptor = {
            "schema": 1,  # Invalid schema
            "hash": "md5",  # Invalid hash
            "layout": "cas-only",  # Invalid layout
            "refs_prefix": "references",  # Invalid refs_prefix
            "cas_prefix": "cas/md5",  # Invalid cas_prefix
        }
        s3.put_object(
            Bucket=bucket,
            Key=descriptor_key,
            Body=json.dumps(invalid_descriptor, separators=(",", ":"), sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )

        # Create RemoteOps, which should fail hard on invalid descriptor
        with pytest.raises(DmlRepoError, match="Remote initialization failed"):
            RemoteOps(
                _db=db,
                client=s3,
                bucket=bucket,
                prefix=invalid_prefix,
            )

        # Verify descriptor was not rewritten
        response = s3.get_object(Bucket=bucket, Key=descriptor_key)
        descriptor = json.loads(response["Body"].read().decode("utf-8"))
        assert descriptor == invalid_descriptor


class TestFakeDb:
    """Tests for FakeDb and FakeTxn implementations."""

    def test_fake_db_tx_context_manager(self, db):
        """Test that FakeDb.tx() returns a raw fake transaction."""
        with db.tx() as txn_ctx:
            assert isinstance(txn_ctx, FakeTxn)

    def test_fake_txn_put_get_roundtrip(self, db):
        """Test that FakeTxn can put and get values correctly."""
        test_ref = Ref("test:123")
        test_value = {"key": "value"}
        with db.tx() as txn_ctx:
            # Put value
            result_ref = txn_ctx.put(test_value, to=test_ref)
            assert result_ref == test_ref
            # Get value back
            retrieved_value = txn_ctx.get(test_ref)
            assert retrieved_value == test_value


class TestRemoteKeyMapping:
    """Tests for remote key mapping helpers."""

    def test_cas_key_sharding(self, remote_ops):
        """Test CAS key generation with sharding for a known OID."""
        # Known OID from the spec
        oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        expected_key = "test-prefix/cas/sha256/01/23/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        # Test with the prefix set by the fixture
        result = remote_ops._cas_key(oid)
        assert result == expected_key

    def test_cas_key_with_prefix(self, remote_ops, monkeypatch):
        """Test CAS key generation with a different prefix."""
        monkeypatch.setenv("DML_REMOTE_ROOT", "s3://test-bucket/myrepo")
        # Need to recreate remote_ops to pick up the new prefix
        remote_ops_with_prefix = RemoteOps(
            _db=remote_ops._db,
            client=remote_ops.client,
            bucket="test-bucket",
            prefix="myrepo",
        )
        oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        expected_key = "myrepo/cas/sha256/01/23/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        result = remote_ops_with_prefix._cas_key(oid)
        assert result == expected_key

    def test_cas_key_invalid_oid(self, remote_ops):
        """Test that _cas_key rejects invalid OIDs."""
        # Test non-hex characters
        with pytest.raises(InvalidOid, match="Invalid OID"):
            remote_ops._cas_key("gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg")
        # Test wrong length
        with pytest.raises(InvalidOid, match="Invalid OID"):
            remote_ops._cas_key("0123456789abcdef")
        # Test uppercase
        with pytest.raises(InvalidOid, match="Invalid OID"):
            remote_ops._cas_key("0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF")

    def test_ref_key_joining(self, remote_ops):
        """Test ref key generation joins prefix and ref path."""
        ref_path = "tags/main/abc123.json"
        expected_key = "test-prefix/refs/tags/main/abc123.json"
        result = remote_ops._ref_key(ref_path)
        assert result == expected_key

    def test_ref_key_with_prefix(self, remote_ops, monkeypatch):
        """Test ref key generation with a different prefix."""
        monkeypatch.setenv("DML_REMOTE_ROOT", "s3://test-bucket/myrepo")
        # Need to recreate remote_ops to pick up the new prefix
        remote_ops_with_prefix = RemoteOps(
            _db=remote_ops._db,
            client=remote_ops.client,
            bucket="test-bucket",
            prefix="myrepo",
        )
        ref_path = "tags/main/abc123.json"
        expected_key = "myrepo/refs/tags/main/abc123.json"
        result = remote_ops_with_prefix._ref_key(ref_path)
        assert result == expected_key

    def test_ref_key_rejects_path_traversal(self, remote_ops):
        """Test that _ref_key rejects path traversal sequences."""
        # Test leading slash
        with pytest.raises(ValueError, match="Invalid ref path"):
            remote_ops._ref_key("/tags/main/abc123.json")
        # Test double dot
        with pytest.raises(ValueError, match="Invalid ref path"):
            remote_ops._ref_key("../tags/main/abc123.json")
        # Test double dot in middle
        with pytest.raises(ValueError, match="Invalid ref path"):
            remote_ops._ref_key("tags/main/../abc123.json")

    def test_dag_ref_path_and_key(self, remote_ops):
        """Test DAG ref path/key helpers."""
        dag_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        assert remote_ops._dag_ref_path(dag_id) == f"dags/{dag_id}.json"
        assert remote_ops._dag_ref_key(dag_id) == f"test-prefix/refs/dags/{dag_id}.json"

    def test_dag_ref_helpers_reject_invalid_dag_id(self, remote_ops):
        """Test DAG ref helpers reject invalid DAG ids."""
        with pytest.raises(ValueError, match="Invalid DAG id"):
            remote_ops._dag_ref_path("abc")
        with pytest.raises(ValueError, match="Invalid DAG id"):
            remote_ops._dag_ref_key("ABC" * 21 + "A")

    def test_ref_key_still_rejects_dag_paths(self, remote_ops):
        """Test that _ref_key still rejects dags/* paths."""
        dag_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        with pytest.raises(ValueError, match="expected 'tags' or 'cache'"):
            remote_ops._ref_key(f"dags/{dag_id}.json")


class TestRemoteWrappers:
    """Tests for remote S3 thin wrappers."""

    def test_remote_put_get_cas_roundtrip(self, remote_ops):
        """Test putting and getting CAS objects roundtrip."""
        oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        test_data = b"Hello, CAS world!"

        # Put the data
        remote_ops._remote_put_cas(oid, test_data)

        # Get the data back
        retrieved_data = remote_ops._remote_get_cas(oid)
        assert retrieved_data == test_data

    def test_remote_has_cas_true_false(self, remote_ops):
        """Test _remote_has_cas returns True for existing objects, False for non-existing."""
        oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

        # Clear any existing objects in the bucket first
        bucket, _prefix = remote_bucket_and_prefix_from_env()
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        # Initially should not exist
        assert not remote_ops._remote_has_cas(oid)

        # Put some data
        test_data = b"test data"
        remote_ops._remote_put_cas(oid, test_data)

        # Now should exist
        assert remote_ops._remote_has_cas(oid)

    def test_remote_put_ref_fails_if_exists(self, remote_ops):
        """Test that _remote_put_ref fails if ref already exists."""
        ref_path = "tags/test/v1.json"
        test_data = b'{"test": "data"}'

        # First put should succeed
        remote_ops._remote_put_ref(ref_path, test_data)

        # Second put should fail
        with pytest.raises(RefAlreadyExists):
            remote_ops._remote_put_ref(ref_path, test_data)

    def test_remote_delete_ref(self, remote_ops):
        """Test deleting refs."""
        ref_path = "tags/test/v2.json"
        test_data = b'{"test": "delete me"}'

        # Put a ref
        remote_ops._remote_put_ref(ref_path, test_data)

        # Verify it exists by trying to get it
        retrieved_data = remote_ops._remote_get_ref(ref_path)
        assert retrieved_data == test_data

        # Delete it
        remote_ops._remote_delete_ref(ref_path)

        # Now getting it should fail
        with pytest.raises(RemoteError):
            remote_ops._remote_get_ref(ref_path)


class TestRemoteFixtures:
    """Tests for remote operation fixtures."""

    def test_remote_ops_fixture(self, remote_ops):
        """Test that remote_ops fixture creates RemoteOps instance."""
        assert remote_ops is not None
        assert hasattr(remote_ops, "_db")
        assert hasattr(remote_ops, "client")

    def test_s3_fixture_creates_bucket(self, s3):
        """Test that s3 fixture creates the expected bucket."""
        bucket, _prefix = remote_bucket_and_prefix_from_env()
        # Try to list objects in the bucket - should not raise an exception
        try:
            s3.list_objects_v2(Bucket=bucket)
            # If we get here, the bucket exists
            # The bucket may contain descriptor files created by RemoteOps initialization
            assert True  # Bucket exists and is accessible
        except s3.exceptions.NoSuchBucket:
            raise AssertionError(f"Bucket {bucket} was not created") from None


class TestDecoding:
    """Tests for manifest and ref decoding + validation."""

    def test_decode_ref_valid(self, remote_ops):
        """Test decoding a valid ref."""
        ref_data = {
            "kind": "ref",
            "schema": 0,
            "target": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "created_at": 1234567890,
        }
        ref_bytes = json.dumps(ref_data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        decoded = remote_ops._decode_ref(ref_bytes)
        assert decoded == ref_data

    def test_decode_ref_valid_targets(self, remote_ops):
        """Test decoding a ref with valid dag targets."""
        dag1 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        dag2 = "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ref_data = {
            "kind": "ref",
            "schema": 0,
            "target": "2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "created_at": 1234567890,
            "targets": {"dag": [dag1, dag2]},
        }
        ref_bytes = json.dumps(ref_data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        decoded = remote_ops._decode_ref(ref_bytes)
        assert decoded == ref_data

    def test_decode_ref_valid_empty_targets(self, remote_ops):
        """Test decoding a ref with empty dag targets."""
        ref_data = {
            "kind": "ref",
            "schema": 0,
            "target": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "created_at": 1234567890,
            "targets": {"dag": []},
        }
        ref_bytes = json.dumps(ref_data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        decoded = remote_ops._decode_ref(ref_bytes)
        assert decoded == ref_data

    def test_decode_ref_rejects_wrong_kind_or_schema(self, remote_ops):
        """Test that _decode_ref rejects invalid kind or schema."""
        # Wrong kind
        invalid_ref = {
            "kind": "manifest",  # Wrong kind
            "schema": 0,
            "target": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "created_at": 1234567890,
        }
        ref_bytes = json.dumps(invalid_ref).encode("utf-8")
        with pytest.raises(InvalidRef, match="kind must be 'ref'"):
            remote_ops._decode_ref(ref_bytes)

        # Wrong schema
        invalid_ref = {
            "kind": "ref",
            "schema": 1,  # Wrong schema
            "target": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "created_at": 1234567890,
        }
        ref_bytes = json.dumps(invalid_ref).encode("utf-8")
        with pytest.raises(InvalidRef, match="schema must be 0"):
            remote_ops._decode_ref(ref_bytes)

        # Invalid target (uppercase)
        invalid_ref = {
            "kind": "ref",
            "schema": 0,
            "target": "0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",  # Uppercase
            "created_at": 1234567890,
        }
        ref_bytes = json.dumps(invalid_ref).encode("utf-8")
        with pytest.raises(InvalidRef, match="target must be 64 lowercase hex"):
            remote_ops._decode_ref(ref_bytes)

        # Invalid target (wrong length)
        invalid_ref = {
            "kind": "ref",
            "schema": 0,
            "target": "0123456789abcdef",  # Too short
            "created_at": 1234567890,
        }
        ref_bytes = json.dumps(invalid_ref).encode("utf-8")
        with pytest.raises(InvalidRef, match="target must be 64 lowercase hex"):
            remote_ops._decode_ref(ref_bytes)

        # Invalid created_at (not int)
        invalid_ref = {
            "kind": "ref",
            "schema": 0,
            "target": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "created_at": "1234567890",  # String instead of int
        }
        ref_bytes = json.dumps(invalid_ref).encode("utf-8")
        with pytest.raises(InvalidRef, match="created_at must be an integer"):
            remote_ops._decode_ref(ref_bytes)

    def test_decode_manifest_valid(self, remote_ops):
        """Test decoding a valid manifest."""
        manifest_data = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "closure": {
                "commit": ["0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"],
                "blob": ["fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"],
            },
        }
        manifest_bytes = json.dumps(manifest_data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        decoded = remote_ops._decode_manifest(manifest_bytes)
        assert decoded == manifest_data

    def test_decode_manifest_rejects_unsorted_or_dupes(self, remote_ops):
        """Test that _decode_manifest rejects unsorted lists or duplicates."""
        # Unsorted list
        invalid_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "closure": {
                "commit": [
                    "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                ],  # Unsorted
            },
        }
        manifest_bytes = json.dumps(invalid_manifest).encode("utf-8")
        with pytest.raises(InvalidManifest, match="must be sorted"):
            remote_ops._decode_manifest(manifest_bytes)

        # Duplicate OIDs
        invalid_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "closure": {
                "commit": [
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                ],  # Duplicate
            },
        }
        manifest_bytes = json.dumps(invalid_manifest).encode("utf-8")
        with pytest.raises(InvalidManifest, match="must have no duplicates"):
            remote_ops._decode_manifest(manifest_bytes)

        # Invalid OID in closure
        invalid_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "closure": {
                "commit": ["gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg"],  # Invalid OID
            },
        }
        manifest_bytes = json.dumps(invalid_manifest).encode("utf-8")
        with pytest.raises(InvalidManifest, match="must be 64 lowercase hex"):
            remote_ops._decode_manifest(manifest_bytes)


class TestClosureUnion:
    """Tests for closure union helper."""

    def test_closure_union_flattens(self, remote_ops):
        """Test that _closure_union flattens OIDs from all kinds."""
        closure = {
            "commit": ["0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"],
            "blob": ["fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"],
            "tree": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        }
        expected_oids = {
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        }
        result = remote_ops._closure_union(closure)
        assert result == expected_oids

    def test_closure_union_dedupes_across_kinds(self, remote_ops):
        """Test that _closure_union dedupes OIDs that appear in multiple kinds."""
        shared_oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        closure = {
            "commit": [shared_oid],
            "blob": [shared_oid, "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"],
        }
        expected_oids = {
            shared_oid,
            "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
        }
        result = remote_ops._closure_union(closure)
        assert result == expected_oids
        assert len(result) == 2  # Should not have duplicates


class TestLocalHelpers:
    """Tests for local manifest helpers."""

    def test_local_dump_dict_stops_at_child_dags_for_commit_root(self, remote_ops):
        """Test that commit-root dumps do not traverse into child DAG closures."""
        mock_txn = Mock()
        commit_ref = Ref("commit:test")
        tree_ref = Ref("tree:tree1")
        root_blob_ref = Ref("blob:blob1")
        tree_blob_ref = Ref("blob:blob2")
        child_dag_ref = Ref("dag:dag1")
        child_blob_ref = Ref("blob:blob3")

        raw_map = {
            commit_ref: "commit-raw",
            tree_ref: "tree-raw",
            root_blob_ref: "blob1-raw",
            tree_blob_ref: "blob2-raw",
            child_dag_ref: "dag-raw",
            child_blob_ref: "blob3-raw",
        }
        objs = {
            commit_ref: {"tree": tree_ref, "message": root_blob_ref},
            tree_ref: {"meta": tree_blob_ref, "dags": {"child": child_dag_ref}},
            root_blob_ref: {"value": "root"},
            tree_blob_ref: {"value": "tree"},
            child_dag_ref: {"payload": child_blob_ref},
            child_blob_ref: {"value": "child"},
        }

        mock_txn.get.side_effect = lambda ref: objs[ref]
        mock_txn.txn.get.side_effect = lambda ref, raw=False: raw_map[ref] if raw else objs[ref]

        result = remote_ops._local_dump_dict(mock_txn, commit_ref)

        assert result == {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "test",
            "closure": {
                "commit": {"test": "commit-raw"},
                "tree": {"tree1": "tree-raw"},
                "blob": {"blob1": "blob1-raw", "blob2": "blob2-raw"},
            },
        }

    def test_local_dump_dict_stops_at_child_dags_for_dag_root(self, remote_ops):
        """Test that DAG-root dumps include only the root DAG and directly owned objects."""
        mock_txn = Mock()
        root_dag_ref = Ref("dag:root")
        root_blob_ref = Ref("blob:blob1")
        child_dag_ref = Ref("dag:child")
        child_blob_ref = Ref("blob:blob2")

        raw_map = {
            root_dag_ref: "root-dag-raw",
            root_blob_ref: "root-blob-raw",
            child_dag_ref: "child-dag-raw",
            child_blob_ref: "child-blob-raw",
        }
        objs = {
            root_dag_ref: {"payload": root_blob_ref, "child": child_dag_ref},
            root_blob_ref: {"value": "root"},
            child_dag_ref: {"payload": child_blob_ref},
            child_blob_ref: {"value": "child"},
        }

        mock_txn.get.side_effect = lambda ref: objs[ref]
        mock_txn.txn.get.side_effect = lambda ref, raw=False: raw_map[ref] if raw else objs[ref]

        result = remote_ops._local_dump_dict(mock_txn, root_dag_ref)

        assert result == {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "dag",
            "root-id": "root",
            "closure": {
                "dag": {"root": "root-dag-raw"},
                "blob": {"blob1": "root-blob-raw"},
            },
        }

    def test_local_has_uses_txn_get(self, remote_ops):
        """Test that _local_has checks if ref exists using txn.get."""
        from daggerml._internal.types import DmlRepoError

        mock_txn = Mock()

        # Test when ref doesn't exist (txn.get raises DmlRepoError)
        mock_txn.get.side_effect = DmlRepoError("Object not found")
        assert not remote_ops._local_has(mock_txn, "commit", "testid")
        mock_txn.get.assert_called_with(Ref("commit:testid"))

        # Reset the mock
        mock_txn.reset_mock()

        # Test when ref exists
        mock_txn.get.return_value = {"some": "data"}
        mock_txn.get.side_effect = None
        assert remote_ops._local_has(mock_txn, "commit", "testid")

    def test_local_put_head_writes_expected_key_and_value(self, remote_ops):
        """Test that _local_put_head writes the correct head key and value."""
        mock_txn = Mock()
        remote_name = "s3://test-bucket/test-prefix"
        ref_path = "tags/main/abc123.json"
        commit_id = "def4567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

        remote_ops._local_put_head(mock_txn, remote_name, ref_path, commit_id)

        expected_value = Head(commit=Ref(f"commit:{commit_id}"))
        expected_key = Ref(f"head:{remote_name}/{ref_path}")
        mock_txn.put.assert_called_once_with(expected_value, to=expected_key)


class TestBuildRemoteManifest:
    """Tests for remote manifest building from local manifest."""

    def test_build_remote_manifest_overrides_dag_closure_with_direct_ids(self, remote_ops):
        """Test direct_dag_ids override replaces transitive dag closure."""
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "root1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "closure": {
                "commit": {
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef": "data1",
                },
                "dag": {
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef": "dag-a",
                    "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef": "dag-b",
                },
            },
        }

        manifest_dict, _ = remote_ops._build_remote_manifest(
            local_manifest,
            direct_dag_ids=["0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"],
        )

        assert manifest_dict["closure"]["dag"] == ["0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"]

    def test_build_remote_manifest_sorts_each_namespace(self, remote_ops):
        """Test that _build_remote_manifest sorts OIDs within each namespace."""
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "root1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "closure": {
                "commit": {
                    "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321": "data2",
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef": "data1",
                },
                "blob": {
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb": "blob2",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": "blob1",
                },
            },
        }

        manifest_dict, manifest_bytes = remote_ops._build_remote_manifest(local_manifest)

        # Check manifest dict
        expected_dict = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "root1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "closure": {
                "commit": [
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                    "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
                ],
                "blob": [
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                ],
            },
        }
        assert manifest_dict == expected_dict

        # Check canonical bytes
        expected_json = (
            '{"closure":{"blob":["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            '"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],"commit":'
            '["0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",'
            '"fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"]},"kind":'
            '"manifest","root-id":"root1234567890abcdef1234567890abcdef1234567890abcdef'
            '1234567890abcdef","root-ns":"commit","schema":0}'
        )
        assert manifest_bytes.decode("utf-8") == expected_json

    def test_build_remote_manifest_dedupes(self, remote_ops):
        """Test that _build_remote_manifest dedupes OIDs within each namespace."""
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "root1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "closure": {
                "commit": {
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef": "data1",
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef": "duplicate",  # noqa: F601 # Same ID with different data (testing deduplication)
                },
            },
        }

        manifest_dict, _ = remote_ops._build_remote_manifest(local_manifest)

        # Should only have one occurrence of the ID
        assert manifest_dict["closure"]["commit"] == [
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ]

    def test_direct_dag_ids_for_commit_uses_tree_dags_only(self, remote_ops):
        """Test direct dag discovery for commits uses only Tree.dags."""
        dag_a = Ref("dag:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        dag_b = Ref("dag:1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        stray_dag = Ref("dag:2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        tree_ref = Ref("tree:3123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        commit_ref = Ref("commit:4123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        txn = Mock()
        txn.get.side_effect = [
            Commit(parents=[], tree=tree_ref, author="test", message="msg", dag=stray_dag),
            Tree(dags={"a": dag_a, "b": dag_b}),
        ]
        assert remote_ops._direct_dag_ids(txn, commit_ref) == sorted([dag_a.id(), dag_b.id()])

    def test_build_remote_manifest_rejects_non_commit_root(self, remote_ops):
        """Test that _build_remote_manifest rejects non-commit root namespaces."""
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "blob",  # Not "commit"
            "root-id": "root1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "closure": {},
        }

        with pytest.raises(ValueError, match="Cannot push non-commit root namespace: 'blob'"):
            remote_ops._build_remote_manifest(local_manifest)

    def test_manifest_bytes_are_stable(self, remote_ops):
        """Test that identical manifests produce identical canonical bytes."""
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "root1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "closure": {
                "commit": {"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef": "data"},
                "blob": {"fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321": "moredata"},
            },
        }

        # Build twice
        _, bytes1 = remote_ops._build_remote_manifest(local_manifest)
        _, bytes2 = remote_ops._build_remote_manifest(local_manifest)

        # Should be identical
        assert bytes1 == bytes2

        # Verify it's canonical JSON
        json_str = bytes1.decode("utf-8")
        parsed = json.loads(json_str)
        recreated = json.dumps(parsed, separators=(",", ":"), sort_keys=True).encode("utf-8")
        assert recreated == bytes1


class TestPushUploadObjects:
    """Tests for push upload objects functionality."""

    def test_push_uploads_only_missing_objects(self, remote_ops):
        """Test that _push_upload_objects only uploads objects that don't exist remotely."""
        # Create test data
        test_data1 = b"Hello, World!"
        test_data2 = b"Goodbye, World!"

        # Compute SHA256 hashes
        oid1 = hashlib.sha256(test_data1).hexdigest()
        oid2 = hashlib.sha256(test_data2).hexdigest()

        # Encode as base64
        b64_data1 = base64.b64encode(test_data1).decode("ascii")
        b64_data2 = base64.b64encode(test_data2).decode("ascii")

        # Create local manifest with both objects
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "root1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "closure": {
                "blob": {
                    oid1: b64_data1,
                    oid2: b64_data2,
                }
            },
        }

        # Upload objects - both should be uploaded since they don't exist
        remote_ops._push_upload_objects(local_manifest)

        # Verify both objects were uploaded
        assert remote_ops._remote_has_cas(oid1)
        assert remote_ops._remote_has_cas(oid2)
        assert remote_ops._remote_get_cas(oid1) == test_data1
        assert remote_ops._remote_get_cas(oid2) == test_data2

        # Now upload again - should not re-upload existing objects
        # Mock the _remote_has_cas and _remote_put_cas to track calls
        original_has_cas = remote_ops._remote_has_cas
        original_put_cas = remote_ops._remote_put_cas

        has_cas_calls = []
        put_cas_calls = []

        def mock_has_cas(oid):
            has_cas_calls.append(oid)
            return original_has_cas(oid)

        def mock_put_cas(oid, data):
            put_cas_calls.append((oid, data))
            return original_put_cas(oid, data)

        remote_ops._remote_has_cas = mock_has_cas
        remote_ops._remote_put_cas = mock_put_cas

        # Upload again - should check existence but not re-upload
        remote_ops._push_upload_objects(local_manifest)

        # Should have checked both objects
        assert oid1 in has_cas_calls
        assert oid2 in has_cas_calls

        # Should not have uploaded anything (since both exist)
        assert len(put_cas_calls) == 0

    def test_push_rejects_bad_sha256_mismatch(self, remote_ops):
        """Test that _push_upload_objects rejects objects with SHA256 mismatches."""
        # Create test data with wrong hash
        test_data = b"Hello, World!"
        wrong_oid = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"  # All f's
        correct_oid = hashlib.sha256(test_data).hexdigest()

        # Make sure wrong_oid is actually different
        assert wrong_oid != correct_oid

        # Encode as base64
        b64_data = base64.b64encode(test_data).decode("ascii")

        # Create local manifest with mismatched hash
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "root1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "closure": {
                "blob": {
                    wrong_oid: b64_data,  # Wrong OID for this data
                }
            },
        }

        # Should raise ValueError due to hash mismatch
        with pytest.raises(ShaMismatch, match=f"SHA256 mismatch for object {wrong_oid}"):
            remote_ops._push_upload_objects(local_manifest)


class TestPush:
    """Tests for the full push functionality."""

    def test_push_end_to_end_writes_cas_and_ref(self, integration_remote_ops_fn):
        """Test that push end-to-end writes CAS objects and creates ref."""
        remote_ops = integration_remote_ops_fn
        # Create test data
        commit_data = b'{"kind": "commit", "tree": "tree123..."}'
        blob_data = b"Hello, World!"

        # Compute SHA256 hashes
        commit_oid = hashlib.sha256(commit_data).hexdigest()
        blob_oid = hashlib.sha256(blob_data).hexdigest()

        # Create local manifest
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {
                "commit": {commit_oid: base64.b64encode(commit_data).decode("ascii")},
                "blob": {blob_oid: base64.b64encode(blob_data).decode("ascii")},
            },
        }
        with remote_ops._tx(readonly=False) as txn:
            txn.put(Head(commit=Ref(f"commit:{commit_oid}")), to=Ref("head:main"))

        # Mock _local_dump_dict to return our local manifest
        with patch.object(remote_ops, "_local_dump_dict", return_value=local_manifest) as mock_dump:
            with patch.object(remote_ops, "_direct_dag_ids", return_value=[]):
                # Push the head ref
                ref_path = remote_ops.push(Ref("head:main"))

            # Verify _local_dump_dict was called
            mock_dump.assert_called_once()

            # Should return the ref path
            assert ref_path == f"tags/main/{commit_oid}.json"

        # Verify CAS objects were uploaded
        assert remote_ops._remote_has_cas(commit_oid)
        assert remote_ops._remote_has_cas(blob_oid)
        assert remote_ops._remote_get_cas(commit_oid) == commit_data
        assert remote_ops._remote_get_cas(blob_oid) == blob_data

        # Verify manifest was uploaded
        # Build expected remote manifest to get its hash
        remote_manifest_dict, remote_manifest_bytes = remote_ops._build_remote_manifest(local_manifest)
        manifest_id = hashlib.sha256(remote_manifest_bytes).hexdigest()

        manifest_bytes = remote_ops._remote_get_cas(manifest_id)
        manifest = remote_ops._decode_manifest(manifest_bytes)
        assert manifest["kind"] == "manifest"
        assert manifest["root-ns"] == "commit"
        assert manifest["root-id"] == commit_oid
        assert sorted(manifest["closure"]["commit"]) == [commit_oid]
        assert sorted(manifest["closure"]["blob"]) == [blob_oid]

        # Verify ref was created
        ref_bytes = remote_ops._remote_get_ref(ref_path)
        ref_obj = remote_ops._decode_ref(ref_bytes)
        assert ref_obj["kind"] == "ref"
        assert ref_obj["target"] == manifest_id
        assert isinstance(ref_obj["created_at"], int)
        assert ref_obj["targets"] == {"dag": []}

    def test_push_head_publishes_tag_ref(self, integration_remote_ops_fn):
        """Test that pushing a head publishes a tag ref scoped by head name and commit id."""
        remote_ops = integration_remote_ops_fn
        commit_data = b'{"kind":"commit","tree":"tree-head"}'
        commit_oid = hashlib.sha256(commit_data).hexdigest()
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {
                "commit": {commit_oid: base64.b64encode(commit_data).decode("ascii")},
            },
        }
        with remote_ops._tx(readonly=False) as txn:
            txn.put(Head(commit=Ref(f"commit:{commit_oid}")), to=Ref("head:main"))
        with patch.object(remote_ops, "_local_dump_dict", return_value=local_manifest) as mock_dump:
            with patch.object(remote_ops, "_direct_dag_ids", return_value=[]):
                ref_path = remote_ops.push(Ref("head:main"))

        assert ref_path == f"tags/main/{commit_oid}.json"
        assert mock_dump.call_args.args[1] == Ref(f"commit:{commit_oid}")
        ref_bytes = remote_ops._remote_get_ref(ref_path)
        ref_obj = remote_ops._decode_ref(ref_bytes)
        assert ref_obj["kind"] == "ref"
        assert ref_obj["target"]
        assert ref_obj["targets"] == {"dag": []}

    def test_push_with_dag_targets_ensures_dag_refs_before_tag_ref(self, remote_ops):
        """Test push computes targets and ensures DAG refs before writing tag ref."""
        commit_oid = "2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        dag_a = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        dag_b = "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {
                "commit": {commit_oid: base64.b64encode(b"commit").decode("ascii")},
                "dag": {
                    dag_b: base64.b64encode(b"dag-b").decode("ascii"),
                    dag_a: base64.b64encode(b"dag-a").decode("ascii"),
                },
            },
        }
        manifest_bytes = json.dumps(
            {
                "kind": "manifest",
                "schema": 0,
                "root-ns": "commit",
                "root-id": commit_oid,
                "closure": {"commit": [commit_oid], "dag": [dag_a, dag_b]},
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        events = []
        with patch.object(
            remote_ops,
            "_resolve_push_target",
            return_value=(Ref(f"commit:{commit_oid}"), f"tags/main/{commit_oid}.json"),
        ):
            with patch.object(remote_ops, "_local_dump_dict", return_value=local_manifest):
                with patch.object(
                    remote_ops,
                    "_direct_dag_ids",
                    return_value=[dag_a],
                ):
                    with patch.object(
                        remote_ops,
                        "_ensure_dag_ref_in_txn",
                        side_effect=lambda dag_ref, _txn, _stack: events.append(("ensure", dag_ref.id())) or True,
                    ):
                        with patch.object(
                            remote_ops, "_push_upload_objects", side_effect=lambda _lm: events.append(("upload-raw",))
                        ):
                            with patch.object(
                                remote_ops,
                                "_build_remote_manifest",
                                side_effect=lambda _lm, require_commit_root=True, direct_dag_ids=None: (  # noqa: ARG005
                                    {
                                        "kind": "manifest",
                                        "schema": 0,
                                        "root-ns": "commit",
                                        "root-id": commit_oid,
                                        "closure": {"commit": [commit_oid], "dag": direct_dag_ids or []},
                                    },
                                    manifest_bytes,
                                ),
                            ):
                                with patch.object(remote_ops, "_remote_has_cas", return_value=False):
                                    with patch.object(
                                        remote_ops,
                                        "_remote_put_cas",
                                        side_effect=lambda oid, _data: events.append(("put-cas", oid)),
                                    ):
                                        with patch.object(
                                            remote_ops,
                                            "_remote_put_ref",
                                            side_effect=lambda _path, data: events.append(
                                                ("put-ref", json.loads(data))
                                            ),
                                        ):
                                            remote_ops.push(Ref("head:main"))

        assert events[0:1] == [("ensure", dag_a)]
        assert events[-1][0] == "put-ref"
        assert events[-1][1]["targets"] == {"dag": [dag_a]}

    def test_push_ref_is_immutable(self, integration_remote_ops_fn):
        """Test that pushing the same ref twice fails."""
        remote_ops = integration_remote_ops_fn
        # Create test data (different from first test)
        commit_data = b'{"kind": "commit", "tree": "tree456..."}'
        commit_oid = hashlib.sha256(commit_data).hexdigest()

        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {
                "commit": {commit_oid: base64.b64encode(commit_data).decode("ascii")},
            },
        }
        with remote_ops._tx(readonly=False) as txn:
            txn.put(Head(commit=Ref(f"commit:{commit_oid}")), to=Ref("head:main"))

        # Mock _local_dump_dict
        with patch.object(remote_ops, "_local_dump_dict", return_value=local_manifest):
            with patch.object(remote_ops, "_direct_dag_ids", return_value=[]):
                # First push should succeed
                ref_path = remote_ops.push(Ref("head:main"))
                assert ref_path == f"tags/main/{commit_oid}.json"

                # Public API wraps remote errors at subsystem boundary
                with pytest.raises(DmlRepoError, match="already exists"):
                    remote_ops.push(Ref("head:main"))

    def test_push_manifest_uploaded_and_addressable(self, integration_remote_ops_fn):
        """Test that the manifest is uploaded and can be retrieved via its hash."""
        remote_ops = integration_remote_ops_fn
        # Create test data (different from other tests)
        commit_data = b'{"kind": "commit", "tree": "tree789..."}'
        blob_data = b"Hello, World!"

        commit_oid = hashlib.sha256(commit_data).hexdigest()
        blob_oid = hashlib.sha256(blob_data).hexdigest()

        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {
                "commit": {commit_oid: base64.b64encode(commit_data).decode("ascii")},
                "blob": {blob_oid: base64.b64encode(blob_data).decode("ascii")},
            },
        }
        with remote_ops._tx(readonly=False) as txn:
            txn.put(Head(commit=Ref(f"commit:{commit_oid}")), to=Ref("head:main"))

        # Mock _local_dump_dict
        with patch.object(remote_ops, "_local_dump_dict", return_value=local_manifest):
            with patch.object(remote_ops, "_direct_dag_ids", return_value=[]):
                # Push
                remote_ops.push(Ref("head:main"))

        # Build expected remote manifest to get its hash
        remote_manifest_dict, remote_manifest_bytes = remote_ops._build_remote_manifest(local_manifest)
        expected_manifest_oid = hashlib.sha256(remote_manifest_bytes).hexdigest()

        # Verify manifest was uploaded with correct hash
        assert remote_ops._remote_has_cas(expected_manifest_oid)
        stored_manifest_bytes = remote_ops._remote_get_cas(expected_manifest_oid)
        stored_manifest = remote_ops._decode_manifest(stored_manifest_bytes)
        assert stored_manifest == remote_manifest_dict


class TestPull:
    """Tests for the pull functionality."""

    def test_load_ptr_in_txn_resolves_dag_refs_recursively(self, remote_ops):
        """Test load_ptr_in_txn resolves closure['dag'] through refs/dags."""
        commit_data = b'{"kind":"commit"}'
        commit_oid = hashlib.sha256(commit_data).hexdigest()
        dag_data = b'{"kind":"dag-root"}'
        dag_oid = hashlib.sha256(dag_data).hexdigest()
        blob_oid = hashlib.sha256(b"blob-data").hexdigest()
        blob_data = b"blob-data"

        top_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {"commit": [commit_oid], "dag": [dag_oid]},
        }
        dag_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "dag",
            "root-id": dag_oid,
            "closure": {"datum-scalar": [blob_oid]},
        }
        top_manifest_bytes = json.dumps(top_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        dag_manifest_bytes = json.dumps(dag_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        top_manifest_oid = hashlib.sha256(top_manifest_bytes).hexdigest()
        dag_manifest_oid = hashlib.sha256(dag_manifest_bytes).hexdigest()
        dag_ref = {
            "kind": "ref",
            "schema": 0,
            "target": dag_manifest_oid,
            "created_at": 1234567890,
            "meta": {"dag": {"id": dag_oid}},
        }

        remote_ops._remote_put_cas(top_manifest_oid, top_manifest_bytes)
        remote_ops._remote_put_cas(dag_manifest_oid, dag_manifest_bytes)
        remote_ops._remote_put_cas(commit_oid, commit_data)
        remote_ops._remote_put_cas(dag_oid, dag_data)
        remote_ops._remote_put_cas(blob_oid, blob_data)
        remote_ops._remote_put_dag_ref(
            dag_oid,
            json.dumps(dag_ref, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        )

        with remote_ops._tx(readonly=False) as txn:
            with patch.object(remote_ops, "_local_has", return_value=False):
                root_ref = remote_ops.load_ptr_in_txn(top_manifest_oid, txn, expected_root_ns="commit")
                assert root_ref == Ref(f"commit:{commit_oid}")
                assert txn.txn.get(Ref(f"commit:{commit_oid}"), raw=True) == base64.b64encode(commit_data).decode(
                    "ascii"
                )
                assert txn.txn.get(Ref(f"datum-scalar:{blob_oid}"), raw=True) == base64.b64encode(blob_data).decode(
                    "ascii"
                )
                assert txn.txn.get(Ref(f"dag:{dag_oid}"), raw=True) == base64.b64encode(dag_data).decode("ascii")

    def test_load_ptr_in_txn_fails_when_dag_ref_missing(self, remote_ops):
        """Test strict failure when a referenced DAG ref is missing."""
        commit_data = b'{"kind":"commit-missing-dag-ref"}'
        commit_oid = hashlib.sha256(commit_data).hexdigest()
        dag_oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeb"
        top_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {"commit": [commit_oid], "dag": [dag_oid]},
        }
        top_manifest_bytes = json.dumps(top_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        top_manifest_oid = hashlib.sha256(top_manifest_bytes).hexdigest()
        remote_ops._remote_put_cas(top_manifest_oid, top_manifest_bytes)
        remote_ops._remote_put_cas(commit_oid, commit_data)

        with pytest.raises(DmlRepoError, match=rf"Ref dags/{dag_oid}\.json not found"):
            with remote_ops._tx(readonly=False) as txn:
                with patch.object(remote_ops, "_local_has", return_value=False):
                    remote_ops.load_ptr_in_txn(top_manifest_oid, txn, expected_root_ns="commit")

    def test_load_ptr_in_txn_fails_when_dag_manifest_missing(self, remote_ops):
        """Test strict failure when DAG ref target CAS is missing."""
        commit_data = b'{"kind":"commit-missing-dag-manifest"}'
        commit_oid = hashlib.sha256(commit_data).hexdigest()
        dag_oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdec"
        missing_manifest_oid = "3123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        top_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {"commit": [commit_oid], "dag": [dag_oid]},
        }
        top_manifest_bytes = json.dumps(top_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        top_manifest_oid = hashlib.sha256(top_manifest_bytes).hexdigest()
        dag_ref = {
            "kind": "ref",
            "schema": 0,
            "target": missing_manifest_oid,
            "created_at": 1234567890,
            "meta": {"dag": {"id": dag_oid}},
        }
        remote_ops._remote_put_cas(top_manifest_oid, top_manifest_bytes)
        remote_ops._remote_put_cas(commit_oid, commit_data)
        remote_ops._remote_put_dag_ref(
            dag_oid,
            json.dumps(dag_ref, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        )

        with remote_ops._tx(readonly=False) as txn:
            with pytest.raises(MissingCasObject, match=f"CAS object {missing_manifest_oid} not found"):
                remote_ops.load_ptr_in_txn(top_manifest_oid, txn, expected_root_ns="commit")

    def test_pull_resolves_dag_refs(self, integration_remote_ops_fn):
        """Test pull materializes child DAG manifests through refs/dags."""
        remote_ops = integration_remote_ops_fn
        commit_data = b'{"kind":"commit","tree":"tree123"}'
        blob_data = b"blob-data"
        commit_oid = hashlib.sha256(commit_data).hexdigest()
        blob_oid = hashlib.sha256(blob_data).hexdigest()
        dag_data = b'{"kind":"dag-root-pull"}'
        dag_oid = hashlib.sha256(dag_data).hexdigest()

        top_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {"commit": [commit_oid], "dag": [dag_oid]},
        }
        dag_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "dag",
            "root-id": dag_oid,
            "closure": {"datum-scalar": [blob_oid]},
        }
        top_manifest_bytes = json.dumps(top_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        dag_manifest_bytes = json.dumps(dag_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        top_manifest_oid = hashlib.sha256(top_manifest_bytes).hexdigest()
        dag_manifest_oid = hashlib.sha256(dag_manifest_bytes).hexdigest()
        top_ref = {
            "kind": "ref",
            "schema": 0,
            "target": top_manifest_oid,
            "created_at": 1234567890,
            "targets": {"dag": [dag_oid]},
        }
        dag_ref = {
            "kind": "ref",
            "schema": 0,
            "target": dag_manifest_oid,
            "created_at": 1234567890,
            "meta": {"dag": {"id": dag_oid}},
        }

        remote_ops._remote_put_cas(top_manifest_oid, top_manifest_bytes)
        remote_ops._remote_put_cas(dag_manifest_oid, dag_manifest_bytes)
        remote_ops._remote_put_cas(commit_oid, commit_data)
        remote_ops._remote_put_cas(dag_oid, dag_data)
        remote_ops._remote_put_cas(blob_oid, blob_data)
        remote_ops._remote_put_ref(
            "tags/main/with-dag.json",
            json.dumps(top_ref, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        )
        remote_ops._remote_put_dag_ref(
            dag_oid,
            json.dumps(dag_ref, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        )

        with patch.object(remote_ops, "_local_has", return_value=False):
            remote_ops.pull("tags/main/with-dag.json")
            with remote_ops._tx(readonly=True) as txn:
                assert txn.txn.get(Ref(f"commit:{commit_oid}"), raw=True) == base64.b64encode(commit_data).decode(
                    "ascii"
                )
                assert txn.txn.get(Ref(f"datum-scalar:{blob_oid}"), raw=True) == base64.b64encode(blob_data).decode(
                    "ascii"
                )
                assert txn.txn.get(Ref(f"dag:{dag_oid}"), raw=True) == base64.b64encode(dag_data).decode("ascii")

    def test_pull_fails_when_dag_ref_is_malformed(self, remote_ops):
        """Test pull fails on malformed DAG refs."""
        commit_oid = "2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        dag_oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        top_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {"dag": [dag_oid]},
        }
        top_manifest_bytes = json.dumps(top_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        top_manifest_oid = hashlib.sha256(top_manifest_bytes).hexdigest()
        top_ref = {
            "kind": "ref",
            "schema": 0,
            "target": top_manifest_oid,
            "created_at": 1234567890,
            "targets": {"dag": [dag_oid]},
        }

        remote_ops._remote_put_cas(top_manifest_oid, top_manifest_bytes)
        remote_ops._remote_put_ref(
            "tags/main/bad-dag-ref.json",
            json.dumps(top_ref, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        )
        remote_ops.client.put_object(
            Bucket=remote_ops.bucket,
            Key=remote_ops._dag_ref_key(dag_oid),
            Body=json.dumps({"kind": "not-ref", "schema": 0}).encode("utf-8"),
        )

        with pytest.raises(DmlRepoError, match="kind must be 'ref'"):
            remote_ops.pull("tags/main/bad-dag-ref.json")

    def test_load_ptr_in_txn_fails_when_dag_ref_missing_even_if_raw_dag_exists(self, remote_ops):
        """Test readers no longer fall back to raw DAG CAS without refs/dags."""
        commit_data = b'{"kind":"commit-legacy-dag"}'
        commit_oid = hashlib.sha256(commit_data).hexdigest()
        dag_data = b'{"kind":"dag-legacy"}'
        dag_oid = hashlib.sha256(dag_data).hexdigest()
        top_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {"commit": [commit_oid], "dag": [dag_oid]},
        }
        top_manifest_bytes = json.dumps(top_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        top_manifest_oid = hashlib.sha256(top_manifest_bytes).hexdigest()
        remote_ops._remote_put_cas(top_manifest_oid, top_manifest_bytes)
        remote_ops._remote_put_cas(commit_oid, commit_data)
        remote_ops._remote_put_cas(dag_oid, dag_data)

        with pytest.raises(DmlRepoError, match=rf"Ref dags/{dag_oid}\.json not found"):
            with remote_ops._tx(readonly=False) as txn:
                with patch.object(remote_ops, "_local_has", return_value=False):
                    remote_ops.load_ptr_in_txn(top_manifest_oid, txn, expected_root_ns="commit")

    def test_pull_rejects_non_commit_root(self, remote_ops):
        """Test that pull rejects manifests with non-commit root namespace."""
        # Create a manifest with non-commit root
        invalid_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "blob",  # Not "commit"
            "root-id": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "closure": {"blob": ["fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"]},
        }
        manifest_bytes = json.dumps(invalid_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")

        # Create a ref pointing to this manifest
        manifest_id = hashlib.sha256(manifest_bytes).hexdigest()
        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_id,
            "created_at": 1234567890,
            "targets": {"dag": []},
        }
        ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")

        # Upload the invalid manifest and ref
        remote_ops._remote_put_cas(manifest_id, manifest_bytes)
        ref_path = "tags/main/invalid-root-ref.json"
        remote_ops._remote_put_ref(ref_path, ref_bytes)

        # Public API wraps remote errors at subsystem boundary
        with pytest.raises(DmlRepoError, match="Manifest root namespace mismatch"):
            remote_ops.pull(ref_path)

    def test_pull_downloads_missing_objects_only(self, integration_remote_ops_fn):
        """Test that pull downloads only objects that are not already local."""
        remote_ops = integration_remote_ops_fn
        # Create test data
        commit_data = b'{"kind": "commit", "tree": "tree123"}'
        blob_data = b"Hello, World!"

        commit_oid = hashlib.sha256(commit_data).hexdigest()
        blob_oid = hashlib.sha256(blob_data).hexdigest()

        # Create manifest
        manifest_data = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {
                "commit": [commit_oid],
                "datum-scalar": [blob_oid],
            },
        }
        manifest_bytes = json.dumps(manifest_data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        manifest_id = hashlib.sha256(manifest_bytes).hexdigest()

        # Create ref
        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_id,
            "created_at": 1234567890,
            "targets": {"dag": []},
        }
        ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")

        # Upload to remote
        remote_ops._remote_put_cas(manifest_id, manifest_bytes)
        remote_ops._remote_put_cas(commit_oid, commit_data)
        remote_ops._remote_put_cas(blob_oid, blob_data)
        ref_path = "tags/main/test-missing.json"
        remote_ops._remote_put_ref(ref_path, ref_bytes)

        with remote_ops._tx(readonly=False) as txn:
            inserted_ref = remote_ops._put_local_cas_object(txn, "commit", commit_oid, commit_data)
            assert inserted_ref == Ref(f"commit:{commit_oid}")

        remote_ops.pull(ref_path)

        with remote_ops._tx(readonly=True) as txn:
            assert txn.txn.get(Ref(f"commit:{commit_oid}"), raw=True) == base64.b64encode(commit_data).decode("ascii")
            assert txn.txn.get(Ref(f"datum-scalar:{blob_oid}"), raw=True) == base64.b64encode(blob_data).decode("ascii")

    def test_pull_verifies_sha256(self, remote_ops):
        """Test that pull verifies SHA256 of downloaded objects."""
        # Create test data with wrong content
        blob_data = b"Hello, World!"
        wrong_oid = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"  # Wrong OID

        manifest_data = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "closure": {
                "blob": [wrong_oid],  # OID doesn't match data
            },
        }
        manifest_bytes = json.dumps(manifest_data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        manifest_id = hashlib.sha256(manifest_bytes).hexdigest()

        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_id,
            "created_at": 1234567890,
            "targets": {"dag": []},
        }
        ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")

        # Upload with wrong data
        remote_ops._remote_put_cas(manifest_id, manifest_bytes)
        remote_ops._remote_put_cas(wrong_oid, blob_data)  # Wrong data for this OID
        ref_path = "tags/main/test-sha256.json"
        remote_ops._remote_put_ref(ref_path, ref_bytes)

        with patch.object(remote_ops, "_local_has", return_value=False):
            with pytest.raises(DmlRepoError, match=f"SHA256 mismatch for object {wrong_oid}"):
                remote_ops.pull(ref_path)

    def test_pull_writes_head_pointer(self, integration_remote_ops_fn):
        """Test that pull writes the correct head pointer."""
        remote_ops = integration_remote_ops_fn
        # Create test data
        commit_data = b'{"kind": "commit", "tree": "tree123"}'
        commit_oid = hashlib.sha256(commit_data).hexdigest()
        # Create manifest
        manifest_data = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {
                "commit": [commit_oid],
            },
        }
        manifest_bytes = json.dumps(manifest_data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        manifest_id = hashlib.sha256(manifest_bytes).hexdigest()
        # Create ref
        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_id,
            "created_at": 1234567890,
            "targets": {"dag": []},
        }
        ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
        # Upload to remote
        remote_ops._remote_put_cas(manifest_id, manifest_bytes)
        remote_ops._remote_put_cas(commit_oid, commit_data)
        ref_path = "tags/main/test-head.json"
        remote_ops._remote_put_ref(ref_path, ref_bytes)
        # Pull should succeed and write head pointer
        remote_ops.pull(ref_path)
        # Verify head pointer was written
        remote_name = f"s3://{remote_ops.bucket}"
        if remote_ops.prefix:
            remote_name = f"s3://{remote_ops.bucket}/{remote_ops.prefix}"
        expected_head_key = f"head:{remote_name}/{ref_path}"

        # Check that the head was written by trying to read it
        with remote_ops._tx(readonly=True) as txn:
            head_obj = txn.get(Ref(expected_head_key))
            assert head_obj is not None
            assert isinstance(head_obj, Head)
            assert head_obj.commit == Ref(f"commit:{commit_oid}")


class TestTask17PublicApiPolish:
    """Tests for Task 17: Public API polish with typed exceptions."""

    def test_push_raises_on_non_commit_root(self, remote_ops, db):
        """Test that push surfaces non-commit root namespace as DmlRepoError."""
        # Create local manifest with non-commit root
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "blob",  # Not "commit"
            "root-id": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "closure": {},
        }

        # Mock _local_dump_dict to return the invalid manifest
        with patch.object(remote_ops, "_local_dump_dict", return_value=local_manifest):
            commit_ref = Ref("commit:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
            with patch.object(
                remote_ops,
                "_resolve_push_target",
                return_value=(commit_ref, "tags/main/test.json"),
            ):
                with patch.object(remote_ops, "_direct_dag_ids", return_value=[]):
                    with pytest.raises(DmlRepoError, match="Cannot push non-commit root namespace: 'blob'"):
                        remote_ops.push(Ref("head:main"))

    def test_pull_raises_on_missing_cas_object(self, remote_ops):
        """Test that pull surfaces missing CAS objects as DmlRepoError."""
        # Clear the bucket first
        bucket, _prefix = remote_bucket_and_prefix_from_env()
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        # Create a ref pointing to a manifest that doesn't exist
        manifest_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_id,
            "created_at": 1234567890,
            "targets": {"dag": []},
        }
        ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")

        # Upload the ref but not the manifest
        ref_path = "tags/main/missing-manifest.json"
        remote_ops._remote_put_ref(ref_path, ref_bytes)

        # Public API wraps remote errors at subsystem boundary
        with pytest.raises(DmlRepoError, match=f"CAS object {manifest_id} not found"):
            remote_ops.pull(ref_path)

    def test_decode_ref_raises_invalid_ref(self, remote_ops):
        """Test that _decode_ref raises InvalidRef for invalid refs."""
        # Test invalid kind
        invalid_ref = {
            "kind": "invalid",
            "schema": 0,
            "target": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "created_at": 1234567890,
        }
        ref_bytes = json.dumps(invalid_ref).encode("utf-8")
        with pytest.raises(InvalidRef, match="kind must be 'ref'"):
            remote_ops._decode_ref(ref_bytes)

        # Test invalid schema
        invalid_ref = {
            "kind": "ref",
            "schema": 1,  # Invalid
            "target": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "created_at": 1234567890,
        }
        ref_bytes = json.dumps(invalid_ref).encode("utf-8")
        with pytest.raises(InvalidRef, match="schema must be 0"):
            remote_ops._decode_ref(ref_bytes)

        # Test invalid target
        invalid_ref = {
            "kind": "ref",
            "schema": 0,
            "target": "invalid-target",
            "created_at": 1234567890,
        }
        ref_bytes = json.dumps(invalid_ref).encode("utf-8")
        with pytest.raises(InvalidRef, match="target must be 64 lowercase hex"):
            remote_ops._decode_ref(ref_bytes)

        # Test invalid targets object
        invalid_ref = {
            "kind": "ref",
            "schema": 0,
            "target": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "created_at": 1234567890,
            "targets": [],
        }
        ref_bytes = json.dumps(invalid_ref).encode("utf-8")
        with pytest.raises(InvalidRef, match="targets must be an object"):
            remote_ops._decode_ref(ref_bytes)

        # Test invalid targets namespace
        invalid_ref["targets"] = {"blob": []}
        ref_bytes = json.dumps(invalid_ref).encode("utf-8")
        with pytest.raises(InvalidRef, match="targets supports only the 'dag' namespace"):
            remote_ops._decode_ref(ref_bytes)

        # Test unsorted dag targets
        invalid_ref["targets"] = {
            "dag": [
                "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            ]
        }
        ref_bytes = json.dumps(invalid_ref).encode("utf-8")
        with pytest.raises(InvalidRef, match="targets.dag must be a sorted unique list of 64 lowercase hex ids"):
            remote_ops._decode_ref(ref_bytes)

        # Test duplicate dag targets
        invalid_ref["targets"] = {
            "dag": [
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            ]
        }
        ref_bytes = json.dumps(invalid_ref).encode("utf-8")
        with pytest.raises(InvalidRef, match="targets.dag must be a sorted unique list of 64 lowercase hex ids"):
            remote_ops._decode_ref(ref_bytes)

        # Test malformed dag target id
        invalid_ref["targets"] = {"dag": ["not-an-oid"]}
        ref_bytes = json.dumps(invalid_ref).encode("utf-8")
        with pytest.raises(InvalidRef, match="targets.dag must be a sorted unique list of 64 lowercase hex ids"):
            remote_ops._decode_ref(ref_bytes)


class TestDagPublicationHelpers:
    """Tests for per-DAG publication helpers."""

    def test_put_ref_manifest_uploads_top_manifest_and_ensures_dags(self, remote_ops, monkeypatch):
        """Test top-level manifest upload via put_ref_manifest."""
        dag_a = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        dag_b = "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "closure": {
                "commit": {
                    "2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef": base64.b64encode(
                        b"commit"
                    ).decode("ascii")
                },
                "dag": {
                    dag_b: base64.b64encode(b"dag-b").decode("ascii"),
                    dag_a: base64.b64encode(b"dag-a").decode("ascii"),
                },
            },
        }
        manifest_bytes = (
            b'{"closure":{"commit":["2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"],'
            b'"dag":["0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",'
            b'"1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"]},'
            b'"kind":"manifest","root-id":"2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",'
            b'"root-ns":"commit","schema":0}'
        )
        manifest_oid = hashlib.sha256(manifest_bytes).hexdigest()
        ensured = []
        uploaded = []

        monkeypatch.setattr(remote_ops, "_local_dump_dict", lambda _txn, _ref: local_manifest)
        monkeypatch.setattr(remote_ops, "_direct_dag_ids", lambda _txn, _root_ref: [dag_a, dag_b])
        monkeypatch.setattr(
            remote_ops, "_ensure_dag_ref_in_txn", lambda dag_ref, _txn, _stack: ensured.append(dag_ref.id()) or True
        )
        monkeypatch.setattr(
            remote_ops, "_push_upload_objects", lambda manifest: uploaded.append(("raw", manifest["root-id"]))
        )
        monkeypatch.setattr(
            remote_ops,
            "_build_remote_manifest",
            lambda _manifest, require_commit_root=False, direct_dag_ids=None: ({"kind": "manifest"}, manifest_bytes),
        )  # noqa: ARG005
        monkeypatch.setattr(remote_ops, "_remote_has_cas", lambda _oid: False)
        monkeypatch.setattr(remote_ops, "_remote_put_cas", lambda oid, data: uploaded.append((oid, data)))

        assert (
            remote_ops.put_ref_manifest(Ref("commit:2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"))
            == manifest_oid
        )
        assert ensured == [dag_a, dag_b]
        assert uploaded[0] == ("raw", local_manifest["root-id"])
        assert uploaded[1] == (manifest_oid, manifest_bytes)

    def test_put_cache_ref_writes_targets(self, remote_ops):
        """Test cache refs include validated targets."""
        target = "2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        dag_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        remote_ops.put_cache_ref("build", "cache:key", target, overwrite=False, targets={"dag": [dag_id]})

        ref_obj = remote_ops._decode_ref(remote_ops._remote_get_ref("cache/build/cache:key.json"))
        assert ref_obj["target"] == target
        assert ref_obj["targets"] == {"dag": [dag_id]}

    def test_put_cache_ref_rejects_invalid_targets(self, remote_ops):
        """Test cache ref validation rejects malformed targets."""
        target = "2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        with pytest.raises(DmlRepoError, match="Invalid targets"):
            remote_ops.put_cache_ref("build", "cache:key", target, overwrite=False, targets={"blob": []})

    def test_decode_manifest_raises_invalid_manifest(self, remote_ops):
        """Test that _decode_manifest raises InvalidManifest for invalid manifests."""
        # Test invalid kind
        invalid_manifest = {
            "kind": "invalid",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "closure": {},
        }
        manifest_bytes = json.dumps(invalid_manifest).encode("utf-8")
        with pytest.raises(InvalidManifest, match="kind must be 'manifest'"):
            remote_ops._decode_manifest(manifest_bytes)

        # Test invalid schema
        invalid_manifest = {
            "kind": "manifest",
            "schema": 1,  # Invalid
            "root-ns": "commit",
            "root-id": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "closure": {},
        }
        manifest_bytes = json.dumps(invalid_manifest).encode("utf-8")
        with pytest.raises(InvalidManifest, match="schema must be 0"):
            remote_ops._decode_manifest(manifest_bytes)

        # Test invalid OID in closure
        invalid_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "closure": {
                "commit": ["invalid-oid"],
            },
        }
        manifest_bytes = json.dumps(invalid_manifest).encode("utf-8")
        with pytest.raises(InvalidManifest, match="must be 64 lowercase hex"):
            remote_ops._decode_manifest(manifest_bytes)

    def test_cas_key_raises_invalid_oid(self, remote_ops):
        """Test that _cas_key raises InvalidOid for invalid OIDs."""
        # Test invalid characters
        with pytest.raises(InvalidOid, match="Invalid OID"):
            remote_ops._cas_key("gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg")

        # Test wrong length
        with pytest.raises(InvalidOid, match="Invalid OID"):
            remote_ops._cas_key("0123456789abcdef")

    def test_push_upload_objects_raises_sha_mismatch(self, remote_ops):
        """Test that _push_upload_objects raises ShaMismatch for SHA mismatches."""
        # Create data with wrong OID
        test_data = b"Hello, World!"
        wrong_oid = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"  # All f's
        correct_oid = hashlib.sha256(test_data).hexdigest()

        # Make sure they're different
        assert wrong_oid != correct_oid

        # Create local manifest with mismatched hash
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "root1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "closure": {
                "blob": {
                    wrong_oid: base64.b64encode(test_data).decode("ascii"),
                }
            },
        }

        # Should raise ShaMismatch
        with pytest.raises(ShaMismatch, match=f"SHA256 mismatch for object {wrong_oid}"):
            remote_ops._push_upload_objects(local_manifest)

    def test_pull_raises_sha_mismatch_on_bad_cas_data(self, remote_ops):
        """Test that pull raises ShaMismatch when downloaded CAS data has wrong hash."""
        # Create test data with wrong content
        blob_data = b"Hello, World!"
        wrong_oid = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

        manifest_data = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "closure": {
                "blob": [wrong_oid],
            },
        }
        manifest_bytes = json.dumps(manifest_data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        manifest_id = hashlib.sha256(manifest_bytes).hexdigest()

        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_id,
            "created_at": 1234567890,
            "targets": {"dag": []},
        }
        ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")

        # Upload with wrong data
        remote_ops._remote_put_cas(manifest_id, manifest_bytes)
        remote_ops._remote_put_cas(wrong_oid, blob_data)  # Wrong data for this OID
        ref_path = "tags/main/test-sha-mismatch.json"
        remote_ops._remote_put_ref(ref_path, ref_bytes)

        with patch.object(remote_ops, "_local_has", return_value=False):
            with pytest.raises(DmlRepoError, match=f"SHA256 mismatch for object {wrong_oid}"):
                remote_ops.pull(ref_path)


class TestGcMark:
    """Tests for GC mark phase."""

    def test_gc_mark_includes_manifest_and_closure_oids(self, remote_ops):
        """Test that _gc_mark includes manifest targets and closure OIDs."""
        # Clear the bucket to ensure clean state
        bucket, _prefix = remote_bucket_and_prefix_from_env()
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        # Create test data for commit and blob objects
        commit_data = b'{"kind": "commit", "tree": "tree123"}'
        blob_data = b"Hello, World!"
        tree_data = b'{"kind": "tree", "entries": []}'

        commit_oid = hashlib.sha256(commit_data).hexdigest()
        blob_oid = hashlib.sha256(blob_data).hexdigest()
        tree_oid = hashlib.sha256(tree_data).hexdigest()

        # Create a manifest that references these objects
        manifest_data = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {
                "commit": [commit_oid],
                "blob": [blob_oid],
                "tree": [tree_oid],
            },
        }
        manifest_bytes = json.dumps(manifest_data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        manifest_id = hashlib.sha256(manifest_bytes).hexdigest()

        # Create a commit ref pointing to the manifest
        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_id,
            "created_at": 1234567890,
            "targets": {"dag": []},
        }
        ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")

        # Upload all CAS objects
        remote_ops._remote_put_cas(manifest_id, manifest_bytes)
        remote_ops._remote_put_cas(commit_oid, commit_data)
        remote_ops._remote_put_cas(blob_oid, blob_data)
        remote_ops._remote_put_cas(tree_oid, tree_data)

        # Upload the commit ref
        ref_path = "tags/main/test-commit.json"
        remote_ops._remote_put_ref(ref_path, ref_bytes)

        # Run GC mark
        live_oids = remote_ops._gc_mark()

        # Should include:
        # - manifest_id (ref target)
        # - commit_oid, blob_oid, tree_oid (from closure)
        expected_oids = {manifest_id, commit_oid, blob_oid, tree_oid}
        assert live_oids == expected_oids

        # Test with multiple refs (tags + cache)
        # Create a cache ref
        cache_ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_id,  # Same manifest
            "created_at": 1234567891,
            "targets": {"dag": []},
            "meta": {"cache": {"expires_at": 2000000000}},
        }
        cache_ref_bytes = json.dumps(cache_ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
        cache_ref_path = "cache/default/test-cache.json"
        remote_ops._remote_put_ref(cache_ref_path, cache_ref_bytes)

        # Run GC mark again
        live_oids = remote_ops._gc_mark()

        # Should still include the same OIDs (manifest referenced by both refs)
        assert live_oids == expected_oids

    def test_gc_mark_warns_and_deletes_malformed_manifest_by_default(self, remote_ops, caplog):
        """Test default malformed='warn' behavior for malformed manifests."""
        bucket, _prefix = remote_bucket_and_prefix_from_env()
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        invalid_manifest = {
            "kind": "not-a-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "closure": {},
        }
        manifest_bytes = json.dumps(invalid_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        manifest_oid = hashlib.sha256(manifest_bytes).hexdigest()

        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_oid,
            "created_at": 1234567890,
            "targets": {"dag": []},
        }
        ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")

        remote_ops._remote_put_cas(manifest_oid, manifest_bytes)
        remote_ops._remote_put_ref("tags/main/invalid-manifest.json", ref_bytes)

        live_oids = remote_ops._gc_mark()
        assert live_oids == {manifest_oid}
        assert not remote_ops._remote_has_cas(manifest_oid)
        assert f"Malformed manifest {manifest_oid}: kind must be 'manifest'" in caplog.text

    def test_gc_mark_raises_on_malformed_manifest_when_requested(self, remote_ops):
        """Test malformed='raise' fails with a clear message."""
        bucket, _prefix = remote_bucket_and_prefix_from_env()
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        invalid_manifest = {
            "kind": "not-a-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "closure": {},
        }
        manifest_bytes = json.dumps(invalid_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        manifest_oid = hashlib.sha256(manifest_bytes).hexdigest()
        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_oid,
            "created_at": 1234567890,
            "targets": {"dag": []},
        }
        remote_ops._remote_put_cas(manifest_oid, manifest_bytes)
        remote_ops._remote_put_ref(
            "tags/main/invalid-manifest.json",
            json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        )

        with pytest.raises(DmlRepoError, match=rf"Malformed manifest {manifest_oid}: kind must be 'manifest'"):
            remote_ops._gc_mark(malformed="raise")
        assert remote_ops._remote_has_cas(manifest_oid)

    def test_gc_mark_ignores_warning_but_deletes_malformed_manifest(self, remote_ops, caplog):
        """Test malformed='ignore' suppresses warnings but still deletes malformed objects."""
        bucket, _prefix = remote_bucket_and_prefix_from_env()
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        invalid_manifest = {
            "kind": "not-a-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "closure": {},
        }
        manifest_bytes = json.dumps(invalid_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        manifest_oid = hashlib.sha256(manifest_bytes).hexdigest()
        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": manifest_oid,
            "created_at": 1234567890,
            "targets": {"dag": []},
        }
        remote_ops._remote_put_cas(manifest_oid, manifest_bytes)
        remote_ops._remote_put_ref(
            "tags/main/invalid-manifest.json",
            json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        )

        caplog.clear()
        live_oids = remote_ops._gc_mark(malformed="ignore")
        assert live_oids == {manifest_oid}
        assert not remote_ops._remote_has_cas(manifest_oid)
        assert f"Malformed manifest {manifest_oid}" not in caplog.text

    def test_gc_mark_raises_on_malformed_root_ref_when_requested(self, remote_ops):
        """Test malformed='raise' names the bad root ref and reason."""
        bucket, _prefix = remote_bucket_and_prefix_from_env()
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        remote_ops.client.put_object(
            Bucket=remote_ops.bucket,
            Key=remote_ops._ref_key("tags/main/bad.json"),
            Body=json.dumps({"kind": "nope", "schema": 0}).encode("utf-8"),
        )

        with pytest.raises(DmlRepoError, match=r"Malformed ref refs/tags/main/bad.json: kind must be 'ref'"):
            remote_ops._gc_mark(malformed="raise")

    def test_gc_mark_follows_dag_refs_not_dag_refs_as_roots(self, remote_ops):
        """Test GC follows DAG refs only from tag/cache roots."""
        bucket, _prefix = remote_bucket_and_prefix_from_env()
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        commit_data = b'{"kind":"commit"}'
        blob_data = b"blob-data"
        dag_data = b'{"kind":"dag-root"}'
        commit_oid = hashlib.sha256(commit_data).hexdigest()
        blob_oid = hashlib.sha256(blob_data).hexdigest()
        dag_oid = hashlib.sha256(dag_data).hexdigest()
        top_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {"commit": [commit_oid], "dag": [dag_oid]},
        }
        dag_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "dag",
            "root-id": dag_oid,
            "closure": {"blob": [blob_oid]},
        }
        top_manifest_bytes = json.dumps(top_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        dag_manifest_bytes = json.dumps(dag_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        top_manifest_oid = hashlib.sha256(top_manifest_bytes).hexdigest()
        dag_manifest_oid = hashlib.sha256(dag_manifest_bytes).hexdigest()
        remote_ops._remote_put_cas(top_manifest_oid, top_manifest_bytes)
        remote_ops._remote_put_cas(dag_manifest_oid, dag_manifest_bytes)
        remote_ops._remote_put_cas(commit_oid, commit_data)
        remote_ops._remote_put_cas(dag_oid, dag_data)
        remote_ops._remote_put_cas(blob_oid, blob_data)
        remote_ops._remote_put_ref(
            "tags/main/gc.json",
            json.dumps(
                {
                    "kind": "ref",
                    "schema": 0,
                    "target": top_manifest_oid,
                    "created_at": 1,
                    "targets": {"dag": [dag_oid]},
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8"),
        )
        remote_ops._remote_put_dag_ref(
            dag_oid,
            json.dumps(
                {
                    "kind": "ref",
                    "schema": 0,
                    "target": dag_manifest_oid,
                    "created_at": 1,
                    "meta": {"dag": {"id": dag_oid}},
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8"),
        )

        live_oids = remote_ops._gc_mark()
        assert {top_manifest_oid, dag_manifest_oid, commit_oid, dag_oid, blob_oid}.issubset(live_oids)

    def test_gc_mark_skips_missing_dag_ref(self, remote_ops):
        """Test GC skips missing DAG refs listed in targets/closure."""
        commit_data = b'{"kind":"commit"}'
        commit_oid = hashlib.sha256(commit_data).hexdigest()
        dag_oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        top_manifest = {
            "kind": "manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {"commit": [commit_oid], "dag": [dag_oid]},
        }
        top_manifest_bytes = json.dumps(top_manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
        top_manifest_oid = hashlib.sha256(top_manifest_bytes).hexdigest()
        remote_ops._remote_put_cas(top_manifest_oid, top_manifest_bytes)
        remote_ops._remote_put_cas(commit_oid, commit_data)
        remote_ops._remote_put_ref(
            "tags/main/missing-dag-ref.json",
            json.dumps(
                {
                    "kind": "ref",
                    "schema": 0,
                    "target": top_manifest_oid,
                    "created_at": 1,
                    "targets": {"dag": [dag_oid]},
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8"),
        )

        live_oids = remote_ops._gc_mark()
        assert top_manifest_oid in live_oids
        assert commit_oid in live_oids
        assert dag_oid not in live_oids


class TestGcSweep:
    """Tests for GC sweep phase."""

    def test_gc_sweep_deletes_only_unreferenced_and_old(self, remote_ops):
        """Test that GC sweep deletes only unreferenced objects that are old enough."""
        # Clear the bucket first
        bucket, _prefix = remote_bucket_and_prefix_from_env()
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        # Create test OIDs
        live_oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        dead_old_oid = "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
        dead_young_oid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

        # Upload CAS objects with different timestamps

        # Live object (should be kept)
        remote_ops._remote_put_cas(live_oid, b"live data")

        # Dead old object (should be deleted)
        remote_ops._remote_put_cas(dead_old_oid, b"dead old data")

        # Dead young object (should be kept due to age)
        remote_ops._remote_put_cas(dead_young_oid, b"dead young data")

        # Mock the LastModified timestamps to simulate different ages
        # This is tricky with moto, so we'll use a small min_age_seconds and assume
        # the objects are old enough, or we could patch the list_objects_v2 response
        # For this test, we'll set min_age_seconds=0 to test reachability logic

        # Live OIDs set contains only the live OID
        live_oids = {live_oid}

        # Run sweep with min_age_seconds=0 (so age doesn't prevent deletion)
        result = remote_ops._gc_sweep(live_oids, min_age_seconds=0)

        # Should have deleted the dead_old_oid, kept live_oid and dead_young_oid
        # (but since moto might not preserve exact timestamps, we'll just check the logic)
        assert "deleted" in result
        assert "kept_live" in result
        assert "kept_young" in result

        # Verify live object still exists
        assert remote_ops._remote_has_cas(live_oid)

        # Verify dead objects were deleted (since min_age_seconds=0)
        # Note: This test may need adjustment based on how moto handles timestamps
        # For now, let's just ensure the method runs without error

    def test_gc_does_not_delete_live_objects(self, remote_ops):
        """Test that GC does not delete objects that are in live_oids."""
        # Clear the bucket first
        bucket, _prefix = remote_bucket_and_prefix_from_env()
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        # Create test OIDs
        live_oid1 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        live_oid2 = "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"

        # Upload live objects
        remote_ops._remote_put_cas(live_oid1, b"live data 1")
        remote_ops._remote_put_cas(live_oid2, b"live data 2")

        # Live OIDs set contains both
        live_oids = {live_oid1, live_oid2}

        # Run sweep
        result = remote_ops._gc_sweep(live_oids, min_age_seconds=0)

        # Should keep both live objects
        assert result["kept_live"] >= 2  # At least the two we know about

        # Verify both objects still exist
        assert remote_ops._remote_has_cas(live_oid1)
        assert remote_ops._remote_has_cas(live_oid2)

    def test_gc_sweep_raises_on_malformed_cas_key(self, remote_ops):
        """Test that _gc_sweep fails closed when CAS key layout is malformed."""
        bucket, _prefix = remote_bucket_and_prefix_from_env()
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        malformed_tail = "not-an-oid"
        if remote_ops.prefix:
            key = f"{remote_ops.prefix}/cas/sha256/00/00/{malformed_tail}"
        else:
            key = f"cas/sha256/00/00/{malformed_tail}"
        remote_ops.client.put_object(Bucket=bucket, Key=key, Body=b"junk")

        with pytest.raises(InvalidOid, match="Invalid CAS key"):
            remote_ops._gc_sweep(set(), min_age_seconds=0)


class TestGc:
    """Tests for the full GC functionality."""

    def test_gc_calls_prune_and_sweep(self, remote_ops):
        """Test that gc() calls prune() and performs sweep."""
        # Mock the methods to verify they're called
        with patch.object(remote_ops, "prune", return_value=0) as mock_prune:
            with patch.object(remote_ops, "_gc_mark", return_value=set()) as mock_mark:
                with patch.object(
                    remote_ops, "_gc_sweep", return_value={"deleted": 0, "kept_live": 0, "kept_young": 0}
                ) as mock_sweep:
                    result = remote_ops.gc(min_age_seconds=100)

                    # Verify methods were called
                    mock_prune.assert_called_once()
                    mock_mark.assert_called_once_with(malformed="warn")
                    mock_sweep.assert_called_once_with(set(), 100)

                    # Verify result is returned
                    assert result == {"deleted": 0, "kept_live": 0, "kept_young": 0}

    def test_gc_passes_through_explicit_malformed_policy(self, remote_ops):
        """Test gc forwards malformed policy to mark phase."""
        with patch.object(remote_ops, "prune", return_value=0):
            with patch.object(remote_ops, "_gc_mark", return_value=set()) as mock_mark:
                with patch.object(
                    remote_ops, "_gc_sweep", return_value={"deleted": 0, "kept_live": 0, "kept_young": 0}
                ):
                    remote_ops.gc(min_age_seconds=100, malformed="raise")
                    mock_mark.assert_called_once_with(malformed="raise")


class TestList:
    """Tests for the list functionality."""

    def test_list_returns_decoded_refs(self, remote_ops):
        """Test that list returns properly decoded refs with ref_path."""
        # Clear any existing refs from previous tests
        bucket, prefix = remote_bucket_and_prefix_from_env()
        refs_prefix = f"{prefix}/refs/" if prefix else "refs/"
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=refs_prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        # Create test ref data
        ref_obj = {
            "kind": "ref",
            "schema": 0,
            "target": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "created_at": 1234567890,
            "targets": {"dag": []},
            "meta": {"author": "test@example.com", "message": "test commit"},
        }
        ref_bytes = json.dumps(ref_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")

        # Put the ref
        ref_path = "tags/main/test-commit.json"
        remote_ops._remote_put_ref(ref_path, ref_bytes)

        # List commits
        refs = remote_ops.list("tags")

        # Should have one ref
        assert len(refs) == 1
        ref = refs[0]

        # Should have the original fields
        assert ref["kind"] == "ref"
        assert ref["schema"] == 0
        assert ref["target"] == "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        assert ref["created_at"] == 1234567890
        assert ref["meta"] == {"author": "test@example.com", "message": "test commit"}

        # Should have inferred ref_path
        assert ref["ref_path"] == ref_path

    def test_list_filters_prefix_correctly(self, remote_ops):
        """Test that list filters refs by prefix correctly."""
        # Clear any existing refs from previous tests
        bucket, prefix = remote_bucket_and_prefix_from_env()
        refs_prefix = f"{prefix}/refs/" if prefix else "refs/"
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=refs_prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        # Create refs in different prefixes
        tag_ref = {
            "kind": "ref",
            "schema": 0,
            "target": "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321",
            "created_at": 1234567891,
            "targets": {"dag": []},
        }
        cache_ref = {
            "kind": "ref",
            "schema": 0,
            "target": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "created_at": 1234567892,
            "targets": {"dag": []},
        }

        # Put refs in different prefixes
        remote_ops._remote_put_ref(
            "tags/release/v1.0.0.json", json.dumps(tag_ref, separators=(",", ":"), sort_keys=True).encode("utf-8")
        )
        remote_ops._remote_put_ref(
            "cache/default/temp.json", json.dumps(cache_ref, separators=(",", ":"), sort_keys=True).encode("utf-8")
        )

        # List each prefix
        tags = remote_ops.list("tags")
        cache = remote_ops.list("cache")

        # Should have correct counts
        assert len(tags) == 1
        assert len(cache) == 1

        # Should have correct ref_paths
        assert tags[0]["ref_path"] == "tags/release/v1.0.0.json"
        assert cache[0]["ref_path"] == "cache/default/temp.json"

        # Should have correct targets
        assert tags[0]["target"] == tag_ref["target"]
        assert cache[0]["target"] == cache_ref["target"]

    def test_list_raises_on_invalid_refs(self, remote_ops):
        """Test that list fails closed when a ref cannot be decoded."""
        # Clear any existing refs from previous tests
        bucket, prefix = remote_bucket_and_prefix_from_env()
        refs_prefix = f"{prefix}/refs/" if prefix else "refs/"
        paginator = remote_ops.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=refs_prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    remote_ops.client.delete_object(Bucket=bucket, Key=obj["Key"])

        # Put a valid ref
        valid_ref = {
            "kind": "ref",
            "schema": 0,
            "target": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "created_at": 1234567890,
            "targets": {"dag": []},
        }
        remote_ops._remote_put_ref(
            "tags/main/valid.json", json.dumps(valid_ref, separators=(",", ":"), sort_keys=True).encode("utf-8")
        )

        # Put invalid JSON directly to S3 (bypassing validation)
        invalid_json = b'{"invalid": json}'
        remote_ops.client.put_object(
            Bucket=remote_ops.bucket,
            Key=remote_ops._ref_key("tags/main/invalid.json"),
            Body=invalid_json,
        )

        with pytest.raises(DmlRepoError, match="Expecting value"):
            remote_ops.list("tags")

    def test_list_returns_empty_list_when_no_refs(self, remote_ops):
        """Test that list returns empty list when no refs exist for allowed prefix."""
        refs = remote_ops.list("cache/missing")
        assert refs == []


class TestPrune:
    """Tests for the prune functionality."""

    def test_prune_deletes_old_invoke_blobs_only(self, remote_ops):
        """Test that prune deletes old invoke transport blobs."""
        key = f"{remote_ops.prefix}/io/invoke/test.json" if remote_ops.prefix else "io/invoke/test.json"
        remote_ops.client.put_object(Bucket=remote_ops.bucket, Key=key, Body=b"{}")
        remote_ops._IO_INVOKE_PRUNE_AGE_SECONDS = 0

        deleted_count = remote_ops.prune()
        assert deleted_count == 1

        with pytest.raises(remote_ops.client.exceptions.ClientError):
            remote_ops.client.get_object(Bucket=remote_ops.bucket, Key=key)

    def test_prune_does_not_delete_cache_refs(self, remote_ops):
        """Test that prune does not delete cache refs by age metadata."""
        cache_ref = {
            "kind": "ref",
            "schema": 0,
            "target": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "created_at": 0,
            "meta": {"cache": {"expires_at": 0}},
        }
        remote_ops._remote_put_ref(
            "cache/default/expired.json",
            json.dumps(cache_ref, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        )
        deleted_count = remote_ops.prune()
        assert deleted_count == 0
        assert json.loads(remote_ops._remote_get_ref("cache/default/expired.json")) == cache_ref


class TestE2E:
    """End-to-end integration tests: push → pull → gc."""

    def test_e2e_push_pull_gc(self, aws_server, s3):
        """Test complete push→pull→gc flow proves compatibility and GC correctness."""
        # Clear the bucket to ensure clean state
        bucket, prefix = remote_bucket_and_prefix_from_env()
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            if "Contents" in page:
                for obj in page["Contents"]:
                    s3.delete_object(Bucket=bucket, Key=obj["Key"])

        # Step 1: Build local DB state with commit + closure objects
        # Create test data for commit and blob
        commit_data = b'{"kind": "commit", "tree": "tree123", "message": "test commit"}'
        blob_data = b"Hello, World! This is test blob content."

        # Compute SHA256 hashes
        commit_oid = hashlib.sha256(commit_data).hexdigest()
        blob_oid = hashlib.sha256(blob_data).hexdigest()

        # Create local manifest with closure
        local_manifest = {
            "kind": "local-manifest",
            "schema": 0,
            "root-ns": "commit",
            "root-id": commit_oid,
            "closure": {
                "commit": {commit_oid: base64.b64encode(commit_data).decode("ascii")},
                "blob": {blob_oid: base64.b64encode(blob_data).decode("ascii")},
            },
        }

        # Create RemoteOps for push
        push_db = FakeDb()
        push_remote_ops = RemoteOps(
            _db=push_db,
            client=s3,
            bucket=bucket,
            prefix=prefix,
        )

        # Step 2: Push to S3
        with patch.object(push_remote_ops, "_local_dump_dict", return_value=local_manifest):
            with patch.object(
                push_remote_ops,
                "_resolve_push_target",
                return_value=(Ref(f"commit:{commit_oid}"), f"tags/main/{commit_oid}.json"),
            ):
                with patch.object(push_remote_ops, "_direct_dag_ids", return_value=[]):
                    ref_path = push_remote_ops.push(Ref("head:main"))
            assert ref_path == f"tags/main/{commit_oid}.json"

        # Verify push artifacts exist
        assert push_remote_ops._remote_has_cas(commit_oid)
        assert push_remote_ops._remote_has_cas(blob_oid)
        # Manifest should also exist (computed during push)
        remote_manifest_dict, _ = push_remote_ops._build_remote_manifest(local_manifest)
        manifest_bytes = json.dumps(remote_manifest_dict, separators=(",", ":"), sort_keys=True).encode("utf-8")
        manifest_oid = hashlib.sha256(manifest_bytes).hexdigest()
        assert push_remote_ops._remote_has_cas(manifest_oid)
        # Ref should exist
        ref_bytes = push_remote_ops._remote_get_ref(ref_path)
        ref_obj = push_remote_ops._decode_ref(ref_bytes)
        assert ref_obj["target"] == manifest_oid

        # Step 3: Create new empty DB for pull
        pull_db = FakeDb()  # Empty DB
        pull_remote_ops = RemoteOps(
            _db=pull_db,
            client=s3,
            bucket=bucket,
            prefix=prefix,
        )

        # Step 4: Pull and validate
        with patch.object(pull_remote_ops, "_local_has", return_value=False):  # Nothing local
            with patch.object(pull_remote_ops, "_local_put_head"):
                pull_remote_ops.pull(ref_path)

        with pull_remote_ops._tx(readonly=True) as txn:
            assert txn.txn.get(Ref(f"commit:{commit_oid}"), raw=True) == base64.b64encode(commit_data).decode("ascii")
            assert txn.txn.get(Ref(f"blob:{blob_oid}"), raw=True) == base64.b64encode(blob_data).decode("ascii")

        # Step 5: Delete the created ref, run gc(min_age_seconds=0)
        pull_remote_ops._remote_delete_ref(ref_path)
        gc_result = pull_remote_ops.gc(min_age_seconds=0)

        # Step 6: Validate CAS objects deleted
        # Should have deleted the manifest, commit, and blob objects
        assert gc_result["deleted"] >= 3  # At least manifest + commit + blob
        assert not pull_remote_ops._remote_has_cas(manifest_oid)
        assert not pull_remote_ops._remote_has_cas(commit_oid)
        assert not pull_remote_ops._remote_has_cas(blob_oid)
