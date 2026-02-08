"""Garbage collection operations for repository maintenance.

Public API:
    GcOps - Class for garbage collection operations
"""

import logging
from collections import defaultdict
from dataclasses import dataclass

from daggerml._internal._db import Ref
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.types import DmlRepoError

logger = logging.getLogger(__name__)


@dataclass
class GcOps(BaseOps):
    """Garbage collection operations for cleaning up orphaned objects."""

    def gc(self) -> dict[str, int]:
        """Perform garbage collection. Remove unreachable objects.
        Returns a dict mapping object types to count removed.
        """
        try:
            orphans = self.list_orphans()
            stats = defaultdict(int)
            # perform deletions in a write transaction
            with self._tx(readonly=False) as txn:
                for ref in orphans:
                    try:
                        if txn.exists(ref):
                            txn.delete(ref)
                            stats[ref.ns()] += 1
                    except Exception:
                        logger.warning(f"Failed to delete orphaned object: {ref}", exc_info=True)
            return dict(stats)
        except Exception as e:
            raise DmlRepoError(f"GC failed: {e}") from e

    def list_orphans(self, heads: list[Ref] | None = None) -> list[Ref]:
        """Identify orphaned objects (not reachable from provided heads).

        Parameters
        ----------
        heads : list[Ref] or None, optional
            Traversal roots to start reachability analysis from. If ``None``,
            all repository `head` and `index` objects are used. If an empty list
            is provided, the underlying database computes orphans across the
            entire database.

        Returns
        -------
        list[Ref]
            A list of references that are not reachable from the provided heads.

        Raises
        ------
        DmlRepoError
            If the operation fails.
        """
        try:
            with self._tx(readonly=True) as txn:
                if heads is None:
                    heads = list(txn.iter("head")) + list(txn.iter("index"))
                if not heads:
                    logger.warning("Listing orphans with no heads; this will clear the repo.")
                # Call the raw DB transaction helper directly (no BaseOps edits required)
                return list(txn.txn.list_orphans(heads))
        except Exception as e:
            raise DmlRepoError(f"Failed to list orphans: {e}") from e
