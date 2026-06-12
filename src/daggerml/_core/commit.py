"""Git-like commit operations for version control functionality.

This module provides a set of operations for managing commits in a version control system.
It includes functionality for merging commits, reverting changes, checking out DAGs, and rebasing commits.
"""

import logging
from typing import Optional, TypedDict, cast
from warnings import warn

from daggerml._core.db import Ref
from daggerml._core.types import Commit, DmlDB, DmlRepoError, Tree, TxnWithValid
from daggerml._core.util import now

logger = logging.getLogger(__name__)


class CommitDescription(TypedDict):
    id: Ref
    parents: list[Ref]
    dags: dict[str, Ref]
    author: str
    message: str
    created: str


class CommitDiffPayload(TypedDict):
    added: dict[str, Ref]
    removed: dict[str, Ref]
    modified: dict[str, tuple[Ref, Ref]]  # name -> (old_ref, new_ref)


class CommitFullDescription(CommitDescription):
    diff: CommitDiffPayload


class CommitOps:
    def _topo_sort(self, *xs, txn: TxnWithValid):
        xs = list(xs)
        result = []
        while xs:
            x = xs.pop(0)
            if x is not None and txn.get(x) and x not in result:
                result.append(x)
                xs = txn.get(x).parents + xs
        return result

    def _merge_base(self, a, b, *, txn: TxnWithValid):
        aa = self._topo_sort(a, txn=txn)
        ab = set(self._topo_sort(b, txn=txn))
        for ref in aa:
            if ref in ab:
                return ref
        raise DmlRepoError(f"No merge base found between {a.id()[:8]} and {b.id()[:8]}")

    def _linear_path(self, ancestor: Ref, descendant: Ref, *, txn: TxnWithValid) -> list[Ref]:
        path = []
        current = descendant
        while current != ancestor:
            commit: Commit = txn.get(current)
            if len(commit.parents) != 1:
                raise DmlRepoError("Can only rebase linear history")
            path.append(current)
            current = commit.parents[0]
        path.reverse()
        return path

    def _diff(self, t1: Ref, t2: Ref, *, txn: TxnWithValid) -> dict:
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

    def _patch(self, tree: Ref, *diffs, txn: TxnWithValid) -> Ref:
        tree_obj: Tree = txn.get(tree)
        dags = dict(tree_obj.dags)
        for diff in diffs:
            for k, _v in diff["rem"].items():
                dags.pop(k, None)
            for k, v in diff["add"].items():
                dags[k] = v
        return txn.put(Tree(dags))

    def _is_ancestor(self, ancestor: Ref, descendant: Ref, *, txn: TxnWithValid) -> bool:
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

    def _reachable(self, start: Ref, *, txn: TxnWithValid) -> set[Ref]:
        stack = [start]
        seen: set[Ref] = set()
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            stack.extend(txn.get(current).parents)
        return seen

    ############################################################
    ################# MERGE, REVERT, AND REBASE ################
    ############################################################
    def diff(self, commit: Ref, relative_to: Ref | None = None, *, db: DmlDB) -> CommitDiffPayload:
        # "added" means the DAG ref exists in commit but not in relative_to.
        # If relative_to is omitted, show the changes introduced by commit
        # relative to its first parent.
        result: CommitDiffPayload = {"added": {}, "removed": {}, "modified": {}}
        with db.tx() as txn:
            c1_obj: Commit = txn.get(commit)
            if relative_to is None:
                if c1_obj.parents:
                    if len(c1_obj.parents) > 1:
                        logger.warning("Multiple parents found for commit; using the first one as the base for diff")
                    relative_to = c1_obj.parents[0]
                else:
                    return result
            c2_obj: Commit = txn.get(relative_to)
            c1_tree: Tree = txn.get(c1_obj.tree)
            c2_tree: Tree = txn.get(c2_obj.tree)
            for name in set(c1_tree.dags) | set(c2_tree.dags):
                ref1 = c1_tree.dags.get(name)
                ref2 = c2_tree.dags.get(name)
                if ref1 == ref2:
                    continue
                if ref1 is not None and ref2 is None:
                    result["added"][name] = ref1
                elif ref2 is not None and ref1 is None:
                    result["removed"][name] = ref2
                elif ref1 is not None and ref2 is not None:
                    result["modified"][name] = (ref2, ref1)
                else:
                    raise RuntimeError("Unexpected case in diff")
        return result

    def show(self, commit: Ref, *, db: DmlDB) -> CommitFullDescription:
        with db.tx() as txn:
            desc = self._describe(commit, txn)
        diff = self.diff(commit, db=db)
        return {**desc, "diff": diff}

    def get_ancestor(self, commit: Ref, n: int, *, db: DmlDB) -> Ref | None:
        with db.tx() as txn:
            current = commit
            for _ in range(n):
                commit_obj: Commit = txn.get(current)
                if not commit_obj.parents:
                    return None
                if len(commit_obj.parents) > 1:
                    logger.warning("Multiple parents found for commit; using the first one as the ancestor")
                current = commit_obj.parents[0]
            return current

    def merge(self, commit1: Ref | None, commit2: Ref | None, user: str, ff_only: bool = False, *, db: DmlDB) -> Ref:
        if commit1 is None:
            if commit2 is None:
                raise DmlRepoError("Cannot merge unresolved revisions")
            return commit2
        if commit2 is None:
            return commit1
        with db.tx() as txn:
            base_tree = None
            try:
                c0 = self._merge_base(commit1, commit2, txn=txn)
            except DmlRepoError:
                c0 = None
                base_tree = txn.put(Tree(dags={}))
            if c0 == commit1:
                return commit2
            if c0 == commit2:
                return commit1
            if ff_only:
                msg = f"Cannot fast-forward merge: {commit1.id()[:8]} and {commit2.id()[:8]} have diverged"
                raise DmlRepoError(msg)
            c1_obj: Commit = txn.get(commit1)
            c2_obj: Commit = txn.get(commit2)
            if c0 is None:
                assert base_tree is not None
                diff_a = self._diff(base_tree, c1_obj.tree, txn=txn)
                diff_b = self._diff(base_tree, c2_obj.tree, txn=txn)
            else:
                base_commit: Commit = txn.get(c0)
                diff_a = self._diff(base_commit.tree, c1_obj.tree, txn=txn)
                diff_b = self._diff(base_commit.tree, c2_obj.tree, txn=txn)
            conflicts = set()
            for name in set(diff_a["add"].keys()).intersection(diff_b["add"].keys()):
                if diff_a["add"][name] != diff_b["add"][name]:
                    conflicts.add(name)
            for name in set(diff_a["rem"].keys()).intersection(diff_b["rem"].keys()):
                if diff_a["rem"][name] != diff_b["rem"][name]:
                    conflicts.add(name)
            conflicts.update(set(diff_a["add"].keys()).intersection(diff_b["rem"].keys()))
            conflicts.update(set(diff_a["rem"].keys()).intersection(diff_b["add"].keys()))
            if conflicts:
                raise DmlRepoError(f"Merge conflicts: {sorted(conflicts)}")
            patch_base = base_tree if c0 is None else base_commit.tree
            merged_tree = self._patch(cast(Ref, patch_base), diff_a, diff_b, txn=txn)
            return txn.put(
                Commit(
                    parents=[commit1, commit2],
                    tree=merged_tree,
                    author=user,
                    message=f"Merge {commit1.id()[:8]} into {commit2.id()[:8]}",
                    created=now(),
                )
            )

    def revert(self, target_commit: Ref, base_commit: Ref, user: str, message: str | None = None, *, db: DmlDB) -> Ref:
        with db.tx() as txn:
            if not self._is_ancestor(target_commit, base_commit, txn=txn):
                raise DmlRepoError(f"Commit {target_commit.id()[:8]} is not an ancestor of {base_commit.id()[:8]}")
            target = txn.get(target_commit)
            if len(target.parents) != 1:
                raise DmlRepoError("Can only revert commits with exactly one parent")
            before_tree = txn.get(txn.get(target.parents[0]).tree)
            after_tree = txn.get(target.tree)
            current_tree = txn.get(txn.get(base_commit).tree)
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
                Commit(
                    parents=[base_commit],
                    tree=new_tree,
                    author=user,
                    message=message or f"Revert {target_commit.id()[:8]}",
                    created=now(),
                )
            )
            return new_commit

    def rebase(self, source, target, user: str, *, db: DmlDB):
        with db.tx() as txn:
            c0 = self._merge_base(source, target, txn=txn)
            if c0 == source:
                return target
            if c0 == target:
                return source
            rebased_parent = target
            for commit_ref in self._linear_path(c0, source, txn=txn):
                commit: Commit = txn.get(commit_ref)
                old_parent = commit.parents[0]
                new_tree = self._patch(
                    txn.get(rebased_parent).tree,
                    self._diff(txn.get(old_parent).tree, commit.tree, txn=txn),
                    txn=txn,
                )
                rebased_parent = txn.put(
                    Commit(
                        parents=[rebased_parent],
                        tree=new_tree,
                        author=user,
                        message=commit.message,
                        created=now(),
                    )
                )
            return rebased_parent

    ############################################################
    ################ DAG CHECKOUT AND MANAGEMENT ##############$
    ############################################################
    def checkout_dag(self, commit: Ref | None, dag: Ref, name: str, user: str, db: DmlDB) -> Ref:
        with db.tx() as txn:
            if dag.ns() != "dag":
                raise DmlRepoError(f"Input '{dag.to}' is not a DAG ref")
            if commit is None:
                tree = Tree(dags={name: dag})
                parents = []
            else:
                tree = cast(Tree, txn.get(txn.get(commit).tree))
                parents = [commit]
            tree.dags[name] = dag
            if name in tree.dags:
                warn(f"DAG name '{name}' already exists in commit; it will be overwritten", UserWarning, stacklevel=2)
            new_commit = txn.put(
                Commit(
                    parents=parents,
                    tree=txn.put(tree),
                    author=user,
                    message=f"Checkout DAG '{dag}' as '{name}'",
                    created=now(),
                )
            )
        return new_commit

    def delete_dag(self, commit: Ref, name: str, user: str, *, db: DmlDB) -> Ref:
        with db.tx() as txn:
            ctx = txn.get_ctx(commit)
            if name not in ctx.tree.dags:
                raise DmlRepoError(f"DAG '{name}' not found in branch commit tree")
            ctx.tree.dags = {k: v for k, v in ctx.tree.dags.items() if k != name}
            ctx.commit.tree = txn.put(ctx.tree)
            ctx.commit.author = user
            ctx.commit.parents = [commit]
            ctx.commit.message = f"Delete DAG '{name}'"
            new_commit_ref = txn.put(ctx.commit)
        return new_commit_ref

    def get_dag(self, commit: Ref, name: str, *, db: DmlDB) -> Optional[Ref]:
        with db.tx() as txn:
            commit_obj = txn.get(commit)
            tree = txn.get(commit_obj.tree)
        if name not in tree.dags:
            raise DmlRepoError(f"DAG '{name}' not found in commit tree")
        return tree.dags.get(name)

    ############################################################
    ######################### QUERYING #########################
    ############################################################
    def _describe(self, commit: Ref, txn: TxnWithValid) -> CommitDescription:
        ctx = txn.get_ctx(commit)
        return cast(
            CommitDescription,
            {
                "id": commit.id(),
                "parents": ctx.commit.parents,
                "dags": ctx.tree.dags,
                "author": ctx.commit.author,
                "message": ctx.commit.message,
                "created": ctx.commit.created,
            },
        )

    def log(self, commit: Ref, *, limit: int = 100, db: DmlDB) -> list[CommitDescription]:
        to_walk = [commit]
        out = []
        with db.tx() as txn:
            while to_walk and len(out) < limit:
                current = to_walk.pop(0)
                commit_obj: Commit = txn.get(current)
                out.append(self._describe(current, txn))
                to_walk.extend(commit_obj.parents)
        return out

    def describe(self, commit: Ref, *, db: DmlDB) -> CommitDescription:
        with db.tx() as txn:
            return self._describe(commit, txn)

    def ahead_behind(self, local: Ref, upstream: Ref, *, db: DmlDB) -> tuple[int, int]:
        with db.tx() as txn:
            local_reachable = self._reachable(local, txn=txn)
            upstream_reachable = self._reachable(upstream, txn=txn)
        return len(local_reachable - upstream_reachable), len(upstream_reachable - local_reachable)
