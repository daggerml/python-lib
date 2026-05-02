"""Branch and index pointer operations."""

from dataclasses import dataclass

from daggerml._internal._db import Ref
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.types import Commit, DmlPointerConflictError, DmlRepoError, Head, Index, Tree


@dataclass
class HeadOps(BaseOps):
    def list_branches(self, *, txn=None) -> list[str]:
        if txn is not None:
            return [ref.id() for ref in txn.iter("head")]
        with self._tx(readonly=True) as owned_txn:
            return self.list_branches(txn=owned_txn)

    def list_indexes(self, *, txn=None) -> list[str]:
        if txn is not None:
            return [ref.id() for ref in txn.iter("index")]
        with self._tx(readonly=True) as owned_txn:
            return self.list_indexes(txn=owned_txn)

    def create_branch(self, branch_name: str, from_commit: Ref | None = None, *, txn=None) -> str:
        if txn is not None:
            self._create_branch(branch_name, from_commit, txn)
            return branch_name
        with self._tx(readonly=False) as owned_txn:
            return self.create_branch(branch_name, from_commit, txn=owned_txn)

    def delete_branch(self, branch_name: str, *, txn=None) -> None:
        if txn is not None:
            self._delete_pointer(self._branch_ref(branch_name), txn)
            return None
        with self._tx(readonly=False) as owned_txn:
            self.delete_branch(branch_name, txn=owned_txn)

    def get_branch_commit(self, branch_name: str, *, txn=None) -> Ref:
        if txn is not None:
            return self._get_pointer_commit(self._branch_ref(branch_name), txn)
        with self._tx(readonly=True) as owned_txn:
            return self.get_branch_commit(branch_name, txn=owned_txn)

    def update_branch_commit(self, branch_name: str, old_commit: Ref, new_commit: Ref, *, txn=None) -> Ref:
        if txn is not None:
            return self._update_pointer_commit(self._branch_ref(branch_name), old_commit, new_commit, txn)
        with self._tx(readonly=False) as owned_txn:
            return self.update_branch_commit(branch_name, old_commit, new_commit, txn=owned_txn)

    def create_index(self, commit_ref: Ref, *, txn=None) -> str:
        if txn is not None:
            return self._create_index(commit_ref, txn).id()
        with self._tx(readonly=False) as owned_txn:
            return self.create_index(commit_ref, txn=owned_txn)

    def delete_index(self, index_id: str, *, txn=None) -> None:
        if txn is not None:
            self._delete_pointer(self._index_ref(index_id), txn)
            return None
        with self._tx(readonly=False) as owned_txn:
            self.delete_index(index_id, txn=owned_txn)

    def get_index_commit(self, index_id: str, *, txn=None) -> Ref:
        if txn is not None:
            return self._get_pointer_commit(self._index_ref(index_id), txn)
        with self._tx(readonly=True) as owned_txn:
            return self.get_index_commit(index_id, txn=owned_txn)

    def list_pointer_roots(self, *, txn=None) -> list[Ref]:
        if txn is not None:
            return [
                *[self._branch_ref(branch_name) for branch_name in self.list_branches(txn=txn)],
                *[self._index_ref(index_id) for index_id in self.list_indexes(txn=txn)],
            ]
        with self._tx(readonly=True) as owned_txn:
            return self.list_pointer_roots(txn=owned_txn)

    def resolve_branch_ref(self, branch_ref: Ref, *, txn=None) -> tuple[str, Ref]:
        if txn is not None:
            if branch_ref.ns() != "head":
                raise DmlRepoError(f"Expected branch ref, got: {branch_ref}")
            branch_name = branch_ref.id()
            return branch_name, self._get_pointer_commit(branch_ref, txn)
        with self._tx(readonly=True) as owned_txn:
            return self.resolve_branch_ref(branch_ref, txn=owned_txn)

    def update_index_commit(self, index_id: str, old_commit: Ref, new_commit: Ref, *, txn=None) -> Ref:
        if txn is not None:
            return self._update_pointer_commit(self._index_ref(index_id), old_commit, new_commit, txn)
        with self._tx(readonly=False) as owned_txn:
            return self.update_index_commit(index_id, old_commit, new_commit, txn=owned_txn)

    @staticmethod
    def _branch_ref(branch_name: str) -> Ref:
        return Ref(f"head:{branch_name}")

    @staticmethod
    def _index_ref(index_id: str) -> Ref:
        return Ref(f"index:{index_id}")

    def _create_branch(self, branch_name: str, from_commit: Ref | None, txn) -> Ref:
        to_ref = self._branch_ref(branch_name)
        if txn.exists(to_ref):
            raise DmlRepoError(f"Branch already exists: {branch_name}")
        target_commit_ref = from_commit
        if target_commit_ref is None:
            tree_ref = txn.put(Tree(dags={}))
            target_commit_ref = txn.put(Commit(tree=tree_ref, parents=[], author="dml", message="Initial commit"))
        self._require_commit(target_commit_ref, txn)
        return txn.put(Head(commit=target_commit_ref), to=to_ref)

    def _create_index(self, commit_ref: Ref, txn) -> Ref:
        self._require_commit(commit_ref, txn)
        return txn.put(Index(commit=commit_ref))

    @staticmethod
    def _delete_pointer(pointer_ref: Ref, txn) -> None:
        if not txn.exists(pointer_ref):
            raise DmlRepoError(f"Pointer does not exist: {pointer_ref}")
        txn.delete(pointer_ref)

    def _get_pointer_commit(self, pointer_ref: Ref, txn) -> Ref:
        if not txn.exists(pointer_ref):
            raise DmlRepoError(f"Pointer does not exist: {pointer_ref}")
        pointer = txn.get(pointer_ref)
        return pointer.commit

    def _update_pointer_commit(self, pointer_ref: Ref, old_commit: Ref, new_commit: Ref, txn) -> Ref:
        self._require_commit(old_commit, txn)
        self._require_commit(new_commit, txn)
        current_commit = self._get_pointer_commit(pointer_ref, txn)
        if current_commit != old_commit:
            # Keep error message concise and under line length limits
            msg = f"Stale pointer update rejected for {pointer_ref}"
            raise DmlPointerConflictError(msg, current_commit=current_commit)
        obj_cls = Head if pointer_ref.ns() == "head" else Index
        return txn.put(obj_cls(commit=new_commit), to=pointer_ref)

    @staticmethod
    def _require_commit(commit_ref: Ref, txn) -> None:
        if commit_ref.ns() != "commit":
            raise DmlRepoError(f"Expected commit ref, got: {commit_ref}")
        if not txn.exists(commit_ref):
            raise DmlRepoError(f"Commit does not exist: {commit_ref}")
