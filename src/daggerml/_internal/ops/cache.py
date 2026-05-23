"""Cache operations for managing computation results.

Public API:
    CacheOps - Class for cache management operations
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

from daggerml._internal._db import Ref
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.types import DmlRepoError


@dataclass
class CacheOps(BaseOps):
    """CRUD operations for managing cached computation results."""

    remote_root: str

    def _remote_ops(self):
        if not self.remote_root:
            raise DmlRepoError("Remote cache context required")
        from daggerml._internal.ops.remote import RemoteOps

        parsed = urlparse(self.remote_root)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise DmlRepoError(f"Invalid remote root URI: {self.remote_root!r}")
        prefix = parsed.path.strip("/")
        return RemoteOps(_db=self._db, bucket=parsed.netloc, prefix=f"{prefix}/dml" if prefix else "dml")

    @staticmethod
    def get_cache_key(argv_ref: Ref, txn) -> str:
        if argv_ref.ns() != "node-argv":
            raise DmlRepoError(f"Expected argv ref for cache key, got: {argv_ref}")
        argv_datum_ref = txn.get(argv_ref).datum_ref(txn)
        if argv_datum_ref.ns() != "datum-list":
            raise DmlRepoError(f"Expected argv list datum ref for cache key, got: {argv_datum_ref}")
        return argv_datum_ref.id()

    def get(self, argv_ref: Ref, txn) -> Optional[Ref]:
        """Get cached result for `argv_ref` within a transaction."""
        remote_ops = self._remote_ops()
        cache_key = self.get_cache_key(argv_ref, txn)
        target = remote_ops.get_cache_ref(cache_key)
        if target is None:
            return None
        return remote_ops.load_ptr_in_txn(target, txn, expected_root_ns="dag")

    def put(self, dag_ref: Ref, *, execution_id: str) -> str:
        """Create or overwrite a cache entry for `dag_ref`."""
        try:
            if dag_ref.ns() != "dag":
                raise DmlRepoError(f"Expected dag ref for cache value, got: {dag_ref}")
            if not isinstance(execution_id, str) or not execution_id:
                raise DmlRepoError("Execution id required for cache entry publication")
            remote_ops = self._remote_ops()
            with self._tx(readonly=True) as txn:
                dag = txn.get(dag_ref)
                argv_ref = dag.argv
                if argv_ref is None:
                    raise DmlRepoError(f"DAG {dag_ref} has no argv, cannot cache")
                cache_key = self.get_cache_key(argv_ref, txn)
                targets = remote_ops._targets_for_root(txn, dag_ref)
            target = remote_ops.put_ref_manifest(dag_ref)
            remote_ops.put_cache_ref(cache_key, target, targets=targets, execution_id=execution_id)
            return cache_key
        except Exception as e:
            raise DmlRepoError(f"Failed to put cache entry: {e}") from e
