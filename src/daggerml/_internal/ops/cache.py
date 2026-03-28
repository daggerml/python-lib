"""Cache operations for managing computation results.

Public API:
    CacheOps - Class for cache management operations
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Optional

from daggerml._internal._db import Ref
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.types import DmlRepoError


@dataclass
class CacheOps(BaseOps):
    """CRUD operations for managing cached computation results."""

    remote_root: Optional[str] = None
    remote_cache: Optional[str] = None

    @staticmethod
    def _split_remote_root(remote_root: str) -> tuple[str, str]:
        if not remote_root.startswith("s3://"):
            raise DmlRepoError(f"Invalid remote root URI: {remote_root!r}")
        rest = remote_root[5:]
        if not rest:
            raise DmlRepoError(f"Invalid remote root URI: {remote_root!r}")
        if "/" not in rest:
            return rest, "dml"
        bucket, prefix = rest.split("/", 1)
        prefix = prefix.strip("/")
        return bucket, f"{prefix}/dml" if prefix else "dml"

    def _remote_ops(self):
        if not self.remote_root or not self.remote_cache:
            raise DmlRepoError("Remote cache context required")
        from daggerml._internal.ops.remote import RemoteOps

        bucket, prefix = self._split_remote_root(self.remote_root)
        return RemoteOps(_db=self._db, bucket=bucket, prefix=prefix)

    def _require_remote_context(self):
        remote_ops = self._remote_ops()
        cache_name = self.remote_cache
        assert cache_name is not None
        return remote_ops, cache_name

    @staticmethod
    def _cache_ref(argv_ref: Ref, txn) -> Ref:
        if argv_ref.ns() != "node-argv":
            raise DmlRepoError(f"Expected argv ref for cache key, got: {argv_ref}")
        argv_datum_ref = txn.get(argv_ref).datum_ref(txn)
        if argv_datum_ref.ns() != "datum-list":
            raise DmlRepoError(f"Expected argv list datum ref for cache key, got: {argv_datum_ref}")
        return Ref(f"cache:{argv_datum_ref.id()}")

    def _put(self, dag_ref: Ref, txn) -> Ref:
        """Create or overwrite a cache entry for `dag_ref` within a transaction."""
        if dag_ref.ns() != "dag":
            raise DmlRepoError(f"Expected dag ref for cache value, got: {dag_ref}")
        dag = txn.get(dag_ref)
        argv_ref = dag.argv
        if argv_ref is None:
            raise DmlRepoError(f"DAG {dag_ref} has no argv, cannot cache")
        cache_ref = self._cache_ref(argv_ref, txn)
        remote_ops, cache_name = self._require_remote_context()
        local_manifest = txn.dump_dict(dag_ref)
        target = remote_ops._put_ref_manifest_from_local_manifest(local_manifest, dag_ref, txn)
        remote_ops.put_cache_ref(
            cache_name,
            cache_ref.to,
            target,
            overwrite=True,
            targets=remote_ops._targets_for_root(txn, dag_ref),
        )
        return cache_ref

    def _get(self, argv_ref: Ref, txn) -> Optional[Ref]:
        """Get cached result for `argv_ref` within a transaction."""
        remote_ops, cache_name = self._require_remote_context()
        cache_ref = self._cache_ref(argv_ref, txn)
        target = remote_ops.get_cache_ref(cache_name, cache_ref.to)
        if target is None:
            return None
        return remote_ops.load_ptr_in_txn(target, txn, expected_root_ns="dag")

    def put(self, dag_ref: Ref) -> Ref:
        """Create or overwrite a cache entry for `dag_ref`."""
        try:
            with self._tx(readonly=True) as txn:
                return self._put(dag_ref, txn)
        except Exception as e:
            raise DmlRepoError(f"Failed to put cache entry: {e}") from e

    def get(self, argv_ref: Ref) -> Optional[Ref]:
        """Get cached result for `argv_ref`."""
        try:
            with self._tx(readonly=False) as txn:
                return self._get(argv_ref, txn)
        except Exception as e:
            raise DmlRepoError(f"Failed to get cache entry: {e}") from e

    def delete(self, argv_ref: Ref) -> bool:
        """Delete cache entry for `argv_ref`, returning whether it existed."""
        try:
            remote_ops, cache_name = self._require_remote_context()
            with self._tx(readonly=True) as txn:
                cache_ref = self._cache_ref(argv_ref, txn)
            return remote_ops.delete_cache_ref(cache_name, cache_ref.to)
        except Exception as e:
            raise DmlRepoError(f"Failed to delete cache entry: {e}") from e

    def list(self, limit: Optional[int] = None) -> Iterator[tuple[Ref, Ref]]:
        """List cache entries as (argv_ref, result_ref) pairs."""
        try:
            remote_ops, cache_name = self._require_remote_context()
            refs = remote_ops.list_cache_refs(cache_name, limit=limit)
            with self._tx(readonly=False) as txn:
                for cache_key, target in refs:
                    dag_ref = remote_ops.load_ptr_in_txn(target, txn, expected_root_ns="dag")
                    yield Ref(cache_key), dag_ref
        except Exception as e:
            raise DmlRepoError(f"Failed to list cache entries: {e}") from e

    def clear(self) -> int:
        """Delete all cache entries, returning the number removed."""
        try:
            remote_ops, cache_name = self._require_remote_context()
            removed = 0
            for cache_key, _target in remote_ops.list_cache_refs(cache_name):
                if remote_ops.delete_cache_ref(cache_name, cache_key):
                    removed += 1
            return removed
        except Exception as e:
            raise DmlRepoError(f"Failed to clear cache entries: {e}") from e
