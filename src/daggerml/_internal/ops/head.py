"""Git-like branch head management operations for repository versioning.

This module provides HeadOps, a class for managing Git-like branch heads with
operations for creating, deleting, switching branches, and comparing heads.
It handles branch lifecycle and head reference management.

Public API:
    HeadOps - Branch head management operations

Private API:
    Helper methods for head storage, retrieval, and metadata management
"""

from dataclasses import dataclass
from typing import List

from daggerml._internal._db import Ref
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.types import Commit, DmlRepoError, Head, Tree


@dataclass
class HeadOps(BaseOps):
    """Git-like branch head management for repository versioning.

    This class provides branch management functionality including head creation,
    deletion, checkout operations, and head comparison. It manages the lifecycle
    of branch references and their associated commits.
    """

    def list(self) -> List[Ref]:
        """List all branch heads.

        Iterates through the heads namespace to collect all head references.

        Returns
        -------
        List[Ref]
            List of references to all branch heads.

        Raises
        ------
        DmlRepoError
            If heads cannot be retrieved from storage.
        """
        heads = []
        with self._tx(readonly=True) as txn:
            for head_ref in txn.iter("head"):
                heads.append(head_ref)
        return heads

    def create(self, branch_name: str, from_head: Ref | None = None) -> Ref:
        """Create new branch from existing head.

        Creates a new branch head that points to the same commit as the source head.
        Validates that the source exists.

        Parameters
        ----------
        branch_name : str
            Name for the new branch.
        from_head : Ref | None
            Reference to source head/commit to branch from (defaults to None for initial commit).

        Returns
        -------
        Ref (Head)
            The newly created head object.

        Raises
        ------
        DmlRepoError
            If source head doesn't exist or creation fails.
        """
        try:
            to_ref = Ref(f"head:{branch_name}")
            with self._tx(readonly=False) as txn:
                if txn.exists(to_ref):
                    raise DmlRepoError(f"Branch already exists: {branch_name}")
                # Validate source exists and get its commit
                if from_head is None:
                    # Create initial commit
                    initial_tree = Tree(dags={})
                    tree_ref = txn.put(initial_tree)
                    initial_commit = Commit(tree=tree_ref, parents=[], author="dml", message="Initial commit")
                    target_commit_ref = txn.put(initial_commit)
                else:
                    if not txn.exists(from_head):
                        raise DmlRepoError(f"Source head does not exist: {from_head}")
                    match from_head.ns():
                        case "head":
                            target_commit_ref = txn.get(from_head).commit
                        case "commit":
                            target_commit_ref = from_head
                        case _:
                            raise DmlRepoError(f"Invalid source namespace for branch creation: {from_head.ns()}")
                # Create new head object
                new_head = Head(commit=target_commit_ref)
                return txn.put(new_head, to=to_ref)
        except Exception as e:
            raise DmlRepoError(f"Failed to create branch '{branch_name}': {e}") from e

    def delete(self, head_ref: Ref) -> None:
        """Delete branch head by reference.

        Removes a branch head from the repository. Prevents deletion of the
        current active branch for safety.

        Parameters
        ----------
        head_ref : Ref
            Reference to the head to delete.

        Raises
        ------
        DmlRepoError
            If head doesn't exist, is the current branch, or deletion fails.
        """
        try:
            if head_ref.ns() != "head":
                raise DmlRepoError(f"Reference is not a head: {head_ref}")
            with self._tx(readonly=False) as txn:
                if not txn.exists(head_ref):
                    raise DmlRepoError(f"Head does not exist: {head_ref}")
                txn.delete(head_ref)
        except Exception as e:
            raise DmlRepoError(f"Failed to delete head: {e}") from e

    def describe(self, head_ref: Ref) -> dict:
        """Describe a head reference and its current commit."""
        if head_ref.ns() != "head":
            raise DmlRepoError(f"Expected head ref, got: {head_ref}")
        with self._tx(readonly=True) as txn:
            head = txn.get(head_ref)
            return {
                "id": head_ref.id(),
                "ref": head_ref,
                "commit": head.commit,
            }
