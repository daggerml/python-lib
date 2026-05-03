"""Git-like commit operations for version control functionality.

This module provides CommitOps, a class for managing git-like commits with
version control operations like merging, rebasing, and commit history traversal.
It handles commit creation, tree management, and DAG operations within commits.

Public API:
    CommitOps - Git-like commit operations
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from daggerml._internal._db import Ref
from daggerml._internal.config import DmlProjectConfig
from daggerml._internal.ops.base_ops import BaseOps
from daggerml._internal.ops.head import HeadOps
from daggerml._internal.revision_uri import RevisionUri, stringify_revision_uri
from daggerml._internal.types import Commit, DmlRepoError, Tree
from daggerml._internal.util import now


@dataclass(frozen=True)
class RevisionResolution:
    commit: Ref
    kind: str
    branch: str | None = None


@dataclass
class CommitOps(BaseOps):
    """Git-like commit operations for version control.

    This class provides version control functionality including commit history
    traversal, merging, rebasing, and DAG management within commits. It extends
    BaseOps to provide commit-specific operations.
    """

    def _topo_sort(self, *xs):
        """Topologically sort commits by ancestry.

        Parameters
        ----------
        *xs : Ref
            Commit references to sort.

        Returns
        -------
        list[Ref]
            Commits in topological order (ancestors before descendants).
        """
        xs = list(xs)
        result = []
        while len(xs):
            x = xs.pop(0)
            with self._tx(readonly=True) as txn:
                if x is not None and txn.get(x) and x not in result:
                    result.append(x)
                    xs = txn.get(x).parents + xs
        return result

    def _merge_base(self, a, b):
        """Find the common ancestor of two commits.

        Parameters
        ----------
        a : Ref
            First commit reference.
        b : Ref
            Second commit reference.

        Returns
        -------
        Ref
            The merge base (common ancestor) commit.
        """
        while True:
            aa = self._topo_sort(a)
            ab = self._topo_sort(b)
            if set(aa).issubset(ab) or len(set(aa).intersection(ab)) == 0:
                return a
            if set(ab).issubset(aa):
                return b
            with self._tx(readonly=True) as txn:
                pivot = txn.get(max(set(aa).difference(ab), key=aa.index))
            assert len(pivot.parents), "no merge base found"
            if len(pivot.parents) == 1:
                return pivot.parents[0]
            a, b = pivot.parents

    def _diff(self, t1: Ref, t2: Ref, txn) -> dict:
        """Calculate diff between two trees.

        Parameters
        ----------
        t1 : Ref
            First tree reference.
        t2 : Ref
            Second tree reference.
        txn : TxnContext
            Transaction context to use.

        Returns
        -------
        dict
            Dictionary with 'add' and 'rem' keys containing DAG changes.
        """
        d1 = txn.get(t1).dags
        d2 = txn.get(t2).dags
        result = {"add": {}, "rem": {}}
        for k in set(d1.keys()).union(d2.keys()):
            if k not in d2:
                result["rem"][k] = d1[k]
            elif k not in d1:
                result["add"][k] = d2[k]
            elif d1[k] != d2[k]:
                result["rem"][k] = d1[k]
                result["add"][k] = d2[k]
        return result

    def _patch(self, tree: Ref, *diffs, txn) -> Ref:
        """Apply diffs to a tree.

        Parameters
        ----------
        tree : Ref
            Tree reference to patch.
        *diffs : dict
            Diff dictionaries to apply.
        txn : TxnContext
            Transaction context to use.

        Returns
        -------
        Ref
            Reference to the patched tree.
        """
        tree_obj: Tree = txn.get(tree)
        dags = dict(tree_obj.dags)
        for diff in diffs:
            for k, _v in diff["rem"].items():
                dags.pop(k, None)
            for k, v in diff["add"].items():
                dags[k] = v
        return txn.put(Tree(dags))

    def _is_ancestor_in_txn(self, ancestor: Ref, descendant: Ref, txn) -> bool:
        stack = [descendant]
        seen = set()
        while stack:
            current = stack.pop()
            if current == ancestor:
                return True
            if current in seen:
                continue
            seen.add(current)
            stack.extend(txn.get(current).parents)
        return False

    def is_ancestor(self, ancestor: Ref, descendant: Ref) -> bool:
        with self._tx(readonly=True) as txn:
            return self._is_ancestor_in_txn(ancestor, descendant, txn)

    @staticmethod
    def _looks_like_commit_id(value: str) -> bool:
        return len(value) == 64 and all(ch in "0123456789abcdef" for ch in value)

    def _resolve_base(self, base: str, *, project_dir: str, txn) -> tuple[Ref, str, str | None]:
        head_ops = HeadOps(_db=self._db)
        if base.startswith("commit:"):
            ref = Ref(base)
            return ref, "commit", None
        if self._looks_like_commit_id(base):
            return Ref(f"commit:{base}"), "commit", None
        if base.startswith("dml://"):
            try:
                return head_ops.get_branch_commit(base, txn=txn), ("tag" if "@" in base else "remote-branch"), None
            except DmlRepoError:
                raise DmlRepoError(f"Revision {base!r} cannot be resolved locally; run fetch first") from None
        if "/" in base:
            remote_name, branch = base.split("/", 1)
            if remote_name != "origin":
                raise DmlRepoError(f"Unknown remote: {remote_name}")
            project = DmlProjectConfig.load(project_dir)
            tracking_uri = stringify_revision_uri(RevisionUri(project.owner, project.name, branch=branch))
            try:
                return head_ops.get_branch_commit(tracking_uri, txn=txn), "remote-branch", None
            except DmlRepoError:
                raise DmlRepoError(f"Revision {base!r} cannot be resolved locally; run fetch first") from None
        branch_name = base
        try:
            return head_ops.get_branch_commit(branch_name, txn=txn), "branch", branch_name
        except DmlRepoError:
            pass
        project = DmlProjectConfig.load(project_dir)
        tag_tracking_uri = stringify_revision_uri(RevisionUri(project.owner, project.name, tag=base))
        try:
            return head_ops.get_branch_commit(tag_tracking_uri, txn=txn), "tag", None
        except DmlRepoError:
            pass
        raise DmlRepoError(f"Revision {base!r} cannot be resolved locally")

    def resolve_revision(
        self,
        value: str,
        *,
        current_branch: str = "main",
        project_dir: str = ".",
    ) -> RevisionResolution:
        base, sep, steps_s = value.partition("~")
        if base == "HEAD":
            base = current_branch
        with self._tx(readonly=True) as txn:
            ref, kind, branch = self._resolve_base(base, project_dir=project_dir, txn=txn)
            if ref.ns() != "commit":
                raise DmlRepoError(f"Resolved non-commit ref: {ref}")
            steps = int(steps_s) if sep else 0
            for _ in range(steps):
                commit = txn.get(ref)
                if not commit.parents:
                    raise DmlRepoError(f"Revision {value!r} walks past root commit")
                ref = commit.parents[0]
            if steps:
                return RevisionResolution(commit=ref, kind="commit", branch=None)
            if kind == "branch":
                return RevisionResolution(commit=ref, kind="branch", branch=branch)
            return RevisionResolution(commit=ref, kind=kind, branch=None)

    def resolve_revision_ref(self, value: str, *, current_branch: str = "main", project_dir: str = ".") -> Ref:
        return self.resolve_revision(value, current_branch=current_branch, project_dir=project_dir).commit

    def _project_dir(self) -> str:
        db_path = Path(self._db.path).resolve()
        if db_path.name == "db" and db_path.parent.name == ".dml":
            return str(db_path.parent.parent)
        return str(db_path)

    def list(self, head: Ref | str, limit: Optional[int] = None) -> Iterator[Ref]:
        """Get commit history starting from head.

        Walks the commit history following parent references from the given
        branch or commit tip. Yields commit references in reverse chronological order
        (newest to oldest).

        Parameters
        ----------
        head : Ref
            Starting commit or branch reference.
        limit : Optional[int]
            Maximum number of commits to return. If None, returns all.

        Yields
        ------
        Ref
            Commit references in reverse chronological order.

        Raises
        ------
        DmlRepoError
            If the starting commit doesn't exist or history traversal fails.
        """
        count = 0
        try:
            current = (
                self.resolve_revision_ref(head, project_dir=self._project_dir()) if isinstance(head, str) else head
            )
            while current and (limit is None or count < limit):
                if current.ns() != "commit":
                    raise DmlRepoError(f"Expected commit reference, got: {current}")
                yield current
                count += 1
                # Get the commit object to find its parent
                with self._tx(readonly=True) as txn:
                    commit = txn.get(current)
                # Move to parent commit (take first parent if multiple)
                if commit.parents:
                    current = commit.parents[0]
                else:
                    # Reached initial commit with no parents
                    break
        except Exception as e:
            raise DmlRepoError(f"Failed to list commits: {e}") from e

    def merge(self, commit1, commit2, user: str) -> Ref:
        """Merge two commits.

        Parameters
        ----------
        commit1 : Ref
            First commit reference.
        commit2 : Ref
            Second commit reference.
        user : str
            Username for commit authorship.

        Returns
        -------
        Ref
            Reference to the merge commit.
        """
        c0 = self._merge_base(commit1, commit2)
        with self._tx(readonly=True) as txn:
            base_commit: Commit = txn.get(c0)
            c1_obj: Commit = txn.get(commit1)
            c2_obj: Commit = txn.get(commit2)

        def merge_trees(base, a, b, txn):
            diff_a = self._diff(base, a, txn)
            diff_b = self._diff(base, b, txn)
            conflicts = []
            for name in set(diff_a["add"].keys()).intersection(diff_b["add"].keys()):
                if diff_a["add"][name] != diff_b["add"][name]:
                    conflicts.append(name)
            for name in set(diff_a["rem"].keys()).intersection(diff_b["rem"].keys()):
                if diff_a["rem"][name] != diff_b["rem"][name]:
                    conflicts.append(name)
            if conflicts:
                raise DmlRepoError(f"Merge conflicts: {sorted(conflicts)}")
            # Apply both diffs
            return self._patch(base, diff_a, diff_b, txn=txn)

        with self._tx(readonly=False) as txn:
            merged_tree = merge_trees(base_commit.tree, c1_obj.tree, c2_obj.tree, txn)
            return txn.put(
                Commit(
                    parents=[commit1, commit2],
                    tree=merged_tree,
                    author=user,
                    message=f"Merge {commit1.id()[:8]} into {commit2.id()[:8]}",
                )
            )

    def merge_into_head(self, branch: str, other: Ref, user: str) -> Ref:
        hops = HeadOps(_db=self._db)
        with self._tx(readonly=False) as txn:
            current = hops.get_branch_commit(branch, txn=txn)
            if self._is_ancestor_in_txn(current, other, txn):
                hops.update_branch_commit(branch, current, other, txn=txn)
                return other
            if self._is_ancestor_in_txn(other, current, txn):
                return current
        merged = self.merge(current, other, user)
        with self._tx(readonly=False) as txn:
            hops.update_branch_commit(branch, current, merged, txn=txn)
        return merged

    def revert(self, branch: str, commit: Ref, user: str) -> Ref:
        if commit.ns() != "commit":
            raise DmlRepoError("Revert expects head and commit refs")
        hops = HeadOps(_db=self._db)
        with self._tx(readonly=False) as txn:
            current_head = hops.get_branch_commit(branch, txn=txn)
            target = txn.get(commit)
            if len(target.parents) != 1:
                raise DmlRepoError("Can only revert commits with exactly one parent")
            before_tree = txn.get(txn.get(target.parents[0]).tree)
            after_tree = txn.get(target.tree)
            current_tree = txn.get(txn.get(current_head).tree)
            dags = dict(current_tree.dags)
            conflicts = []
            for name in set(before_tree.dags) | set(after_tree.dags):
                before_ref = before_tree.dags.get(name)
                after_ref = after_tree.dags.get(name)
                if before_ref == after_ref:
                    continue
                if dags.get(name) != after_ref:
                    conflicts.append(name)
                    continue
                if before_ref is None:
                    dags.pop(name, None)
                else:
                    dags[name] = before_ref
            if conflicts:
                raise DmlRepoError(f"Revert conflicts: {sorted(set(conflicts))}")
            new_tree = txn.put(Tree(dags=dags))
            new_commit = txn.put(
                Commit(parents=[current_head], tree=new_tree, author=user, message=f"Revert {commit.id()[:8]}")
            )
            hops.update_branch_commit(branch, current_head, new_commit, txn=txn)
            return new_commit

    def checkout_dag(
        self,
        branch: str,
        source_commit: Ref,
        source_name: str,
        *,
        target_name: str | None = None,
        replace: bool = False,
        user: str,
    ) -> Ref:
        hops = HeadOps(_db=self._db)
        target_name = target_name or source_name
        with self._tx(readonly=False) as txn:
            current_commit_ref = hops.get_branch_commit(branch, txn=txn)
            current_commit = txn.get(current_commit_ref)
            source_tree = txn.get(txn.get(source_commit).tree)
            if source_name not in source_tree.dags:
                raise DmlRepoError(f"DAG '{source_name}' not found in source commit")
            dag_ref = source_tree.dags[source_name]
            current_tree = txn.get(current_commit.tree)
            if target_name in current_tree.dags and current_tree.dags[target_name] != dag_ref and not replace:
                raise DmlRepoError(f"DAG '{target_name}' already exists; use --replace")
            dags = dict(current_tree.dags)
            dags[target_name] = dag_ref
            new_tree = txn.put(Tree(dags=dags))
            new_commit = txn.put(
                Commit(
                    parents=[current_commit_ref],
                    tree=new_tree,
                    author=user,
                    message=f"Checkout DAG '{source_name}' from {source_commit.id()[:8]}",
                    dag=dag_ref,
                )
            )
            hops.update_branch_commit(branch, current_commit_ref, new_commit, txn=txn)
            return new_commit

    def rebase(self, source, target, user: str):
        """Rebase source commit onto target.

        Parameters
        ----------
        source : Ref
            Commit to rebase.
        target : Ref
            Target commit to rebase onto.
        user : str
            Username for commit authorship.

        Returns
        -------
        Ref
            Reference to the rebased commit.
        """
        with self._tx(readonly=False) as txn:
            c0 = self._merge_base(source, target)

            def replay(commit_ref, target, txn):
                commit: Commit = txn.get(commit_ref)
                if len(commit.parents) != 1:
                    raise DmlRepoError("Can only rebase linear history")
                old_parent = commit.parents[0]
                new_tree = self._patch(
                    txn.get(target).tree,
                    self._diff(txn.get(old_parent).tree, commit.tree, txn),
                    txn=txn,
                )
                return txn.put(
                    Commit(
                        parents=[target],
                        tree=new_tree,
                        author=user,
                        message=commit.message,
                        dag=commit.dag,
                        created=commit.created,
                        modified=now(),
                    )
                )

            return target if c0 == source else source if c0 == target else replay(source, target, txn)

    def get_dag(self, commit: Ref, name: str) -> Optional[Ref]:
        """Get DAG from commit's tree by name.

        Looks up a named DAG in the commit's tree structure.

        Parameters
        ----------
        commit : Ref
            Commit to search in.
        name : str
            Name of the DAG to find.

        Returns
        -------
        Optional[Ref]
            Reference to the DAG if found, None otherwise.

        Raises
        ------
        DmlRepoError
            If commit doesn't exist or tree lookup fails.
        """
        try:
            with self._tx(readonly=True) as txn:
                commit_obj = txn.get(commit)
            if not isinstance(commit_obj, Commit):
                raise DmlRepoError(f"Expected Commit at {commit}, got {type(commit_obj)}")
            with self._tx(readonly=True) as txn:
                tree = txn.get(commit_obj.tree)
            if not isinstance(tree, Tree):
                raise DmlRepoError(f"Expected Tree at {commit_obj.tree}, got {type(tree)}")
            return tree.dags.get(name)
        except Exception as e:
            raise DmlRepoError(f"Failed to get DAG '{name}' from commit: {e}") from e

    def describe(self, commit: Ref) -> dict:
        """Describe a commit by reference."""
        if commit.ns() != "commit":
            raise DmlRepoError(f"Expected commit reference, got: {commit}")
        with self._tx(readonly=True) as txn:
            commit_obj: Commit = txn.get(commit)
        if not isinstance(commit_obj, Commit):
            raise DmlRepoError(f"Expected Commit at {commit}, got {type(commit_obj)}")
        return {
            "id": commit.id(),
            "parents": commit_obj.parents,
            "tree": commit_obj.tree,
            "author": commit_obj.author,
            "message": commit_obj.message,
            "dag": commit_obj.dag,
            "created": commit_obj.created,
            "modified": commit_obj.modified,
        }

    # FIXME: Move to HeadOps.delete_dag.
    def delete_dag(self, name: str, branch: str, user: str) -> Self:
        """Remove DAG from head's tree and create new commit.

        Creates a new commit with the specified DAG removed from the tree.
        Uses the current branch commit as the parent of the new commit.

        Parameters
        ----------
        name : str
            Name of the DAG to remove.
        head : Ref
            Branch name to modify.
        user : str
            Username for commit authorship.

        Returns
        -------
        Ref
            Reference to the new commit with DAG removed.

        Raises
        ------
        DmlRepoError
            If the branch commit/DAG doesn't exist or deletion fails.
        """
        try:
            hops = HeadOps(_db=self._db)
            with self._tx(readonly=False) as txn:
                current_commit_ref = hops.get_branch_commit(branch, txn=txn)
                ctx = txn.get_commit_ctx(current_commit_ref)
                # Check if DAG exists
                if name not in ctx.tree.dags:
                    raise DmlRepoError(f"DAG '{name}' not found in branch commit tree")
                # Create new tree without the specified DAG
                ctx.tree.dags = {k: v for k, v in ctx.tree.dags.items() if k != name}
                ctx.commit.tree = txn.put(ctx.tree)
                ctx.commit.author = user
                ctx.commit.parents = [current_commit_ref]
                ctx.commit.message = f"Delete DAG '{name}'"
                new_commit_ref = txn.put(ctx.commit)
                hops.update_branch_commit(branch, current_commit_ref, new_commit_ref, txn=txn)
            return self
        except Exception as e:
            raise DmlRepoError(f"Failed to delete DAG '{name}': {e}") from e
