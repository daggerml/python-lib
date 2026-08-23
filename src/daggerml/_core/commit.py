"""Git-like commit operations for version control functionality.

This module provides a set of operations for managing commits in a version control system.
It includes functionality for merging commits, reverting changes, checking out DAGs, and rebasing commits.
"""

import logging
from typing import Optional, TypedDict, cast

from daggerml._core.db import Ref
from daggerml._core.types import Commit, DmlDB, DmlRepoError, Tree, TxnWithValid
from daggerml._core.util import now

logger = logging.getLogger(__name__)


class ShallowHistoryError(DmlRepoError):
    def __init__(self, commit: Ref):
        super().__init__(f"Commit history is shallow at {commit}; fetch with greater depth or --unshallow")


class CommitDescription(TypedDict):
    id: Ref
    parents: list[Ref]
    dags: dict[str, Ref]
    tags: dict[str, list[str]]
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
    @staticmethod
    def _get_commit(ref: Ref, *, txn: TxnWithValid, missing_commits: set[Ref] | None = None) -> Commit:
        if not txn.exists(ref) and ref in (missing_commits or set()):
            raise ShallowHistoryError(ref)
        return cast(Commit, txn.get(ref))

    @staticmethod
    def _entries(tree: Tree) -> dict[str, tuple[Ref, list[str]]]:
        return {name: (dag, tree.tags.get(name, [])) for name, dag in tree.dags.items()}

    def _topo_sort(self, *xs, txn: TxnWithValid, missing_commits: set[Ref] | None = None):
        xs = list(xs)
        result = []
        while xs:
            x = xs.pop(0)
            if x is not None and x not in result:
                commit = self._get_commit(x, txn=txn, missing_commits=missing_commits)
                result.append(x)
                xs = commit.parents + xs
        return result

    def _merge_base(self, a, b, *, txn: TxnWithValid, missing_commits: set[Ref] | None = None):
        def available(start: Ref) -> tuple[list[Ref], ShallowHistoryError | None]:
            pending = [start]
            result = []
            shallow_error = None
            while pending:
                current = pending.pop(0)
                if current in result:
                    continue
                if not txn.exists(current) and current in (missing_commits or set()):
                    shallow_error = ShallowHistoryError(current)
                    continue
                commit = self._get_commit(current, txn=txn, missing_commits=missing_commits)
                result.append(current)
                pending.extend(commit.parents)
            return result, shallow_error

        aa, shallow_a = available(a)
        available_b, shallow_b = available(b)
        ab = set(available_b)
        for ref in aa:
            if ref in ab:
                return ref
        if shallow_a is not None:
            raise shallow_a
        if shallow_b is not None:
            raise shallow_b
        raise DmlRepoError(f"No merge base found between {a.id()[:8]} and {b.id()[:8]}")

    def _linear_path(
        self,
        ancestor: Ref,
        descendant: Ref,
        *,
        txn: TxnWithValid,
        missing_commits: set[Ref] | None = None,
    ) -> list[Ref]:
        path = []
        current = descendant
        while current != ancestor:
            commit = self._get_commit(current, txn=txn, missing_commits=missing_commits)
            if len(commit.parents) != 1:
                raise DmlRepoError("Can only rebase linear history")
            path.append(current)
            current = commit.parents[0]
        path.reverse()
        return path

    def _diff(self, t1: Ref, t2: Ref, *, txn: TxnWithValid) -> dict:
        d1 = self._entries(txn.get(t1))
        d2 = self._entries(txn.get(t2))
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
        tags = dict(tree_obj.tags)
        for diff in diffs:
            for k, _v in diff["rem"].items():
                dags.pop(k, None)
                tags.pop(k, None)
            for k, (dag, dag_tags) in diff["add"].items():
                dags[k] = dag
                if dag_tags:
                    tags[k] = dag_tags
                else:
                    tags.pop(k, None)
        return txn.put(Tree(dags=dags, tags=tags))

    def _is_ancestor(
        self,
        ancestor: Ref,
        descendant: Ref,
        *,
        txn: TxnWithValid,
        missing_commits: set[Ref] | None = None,
    ) -> bool:
        stack = [descendant]
        seen = set()
        while stack:
            current = stack.pop()
            if current == ancestor:
                return True
            if current in seen:
                continue
            seen.add(current)
            stack.extend(self._get_commit(current, txn=txn, missing_commits=missing_commits).parents)
        return False

    def is_ancestor(
        self,
        ancestor: Ref,
        descendant: Ref,
        *,
        db: DmlDB,
        missing_commits: set[Ref] | None = None,
    ) -> bool:
        """Return whether ``ancestor`` is reachable from ``descendant``."""
        with db.tx(readonly=True) as txn:
            return self._is_ancestor(ancestor, descendant, txn=txn, missing_commits=missing_commits)

    def _reachable(
        self, start: Ref, *, txn: TxnWithValid, missing_commits: set[Ref] | None = None
    ) -> set[Ref]:
        stack = [start]
        seen: set[Ref] = set()
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            stack.extend(self._get_commit(current, txn=txn, missing_commits=missing_commits).parents)
        return seen

    ############################################################
    ################# MERGE, REVERT, AND REBASE ################
    ############################################################
    def diff(
        self,
        commit: Ref,
        relative_to: Ref | None = None,
        *,
        db: DmlDB,
        missing_commits: set[Ref] | None = None,
    ) -> CommitDiffPayload:
        # "added" means the DAG ref exists in commit but not in relative_to.
        # If relative_to is omitted, show the changes introduced by commit
        # relative to its first parent.
        result: CommitDiffPayload = {"added": {}, "removed": {}, "modified": {}}
        with db.tx(readonly=True) as txn:
            c1_obj = self._get_commit(commit, txn=txn, missing_commits=missing_commits)
            if relative_to is None:
                if c1_obj.parents:
                    if len(c1_obj.parents) > 1:
                        logger.warning("Multiple parents found for commit; using the first one as the base for diff")
                    relative_to = c1_obj.parents[0]
                else:
                    return result
            c2_obj = self._get_commit(relative_to, txn=txn, missing_commits=missing_commits)
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

    def show(
        self, commit: Ref, *, db: DmlDB, missing_commits: set[Ref] | None = None
    ) -> CommitFullDescription:
        with db.tx(readonly=True) as txn:
            desc = self._describe(commit, txn)
        diff = self.diff(commit, db=db, missing_commits=missing_commits)
        return {**desc, "diff": diff}

    def get_ancestor(
        self, commit: Ref, n: int, *, db: DmlDB, missing_commits: set[Ref] | None = None
    ) -> Ref | None:
        with db.tx(readonly=True) as txn:
            current = commit
            for _ in range(n):
                commit_obj = self._get_commit(current, txn=txn, missing_commits=missing_commits)
                if not commit_obj.parents:
                    return None
                if len(commit_obj.parents) > 1:
                    logger.warning("Multiple parents found for commit; using the first one as the ancestor")
                current = commit_obj.parents[0]
            self._get_commit(current, txn=txn, missing_commits=missing_commits)
            return current

    def merge(
        self,
        commit1: Ref | None,
        commit2: Ref | None,
        user: str,
        ff_only: bool = False,
        *,
        db: DmlDB,
        missing_commits: set[Ref] | None = None,
    ) -> Ref:
        if commit1 is None:
            if commit2 is None:
                raise DmlRepoError("Cannot merge unresolved revisions")
            return commit2
        if commit2 is None:
            return commit1
        created = now()

        def merge_commits(txn: TxnWithValid) -> Ref:
            base_tree = None
            try:
                c0 = self._merge_base(commit1, commit2, txn=txn, missing_commits=missing_commits)
            except ShallowHistoryError:
                raise
            except DmlRepoError:
                c0 = None
                base_tree = txn.put(Tree(dags={}, tags={}))
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
                    created=created,
                )
            )

        return db.write_with_growth(merge_commits)

    def revert(
        self,
        target_commit: Ref,
        base_commit: Ref,
        user: str,
        message: str | None = None,
        *,
        db: DmlDB,
        missing_commits: set[Ref] | None = None,
    ) -> Ref:
        created = now()

        def revert_commit(txn: TxnWithValid) -> Ref:
            if not self._is_ancestor(
                target_commit, base_commit, txn=txn, missing_commits=missing_commits
            ):
                raise DmlRepoError(f"Commit {target_commit.id()[:8]} is not an ancestor of {base_commit.id()[:8]}")
            target = self._get_commit(target_commit, txn=txn, missing_commits=missing_commits)
            if len(target.parents) != 1:
                raise DmlRepoError("Can only revert commits with exactly one parent")
            parent = self._get_commit(target.parents[0], txn=txn, missing_commits=missing_commits)
            before_tree = txn.get(parent.tree)
            after_tree = txn.get(target.tree)
            current_tree = txn.get(txn.get(base_commit).tree)
            dags = dict(current_tree.dags)
            tags = dict(current_tree.tags)
            conflicts = []
            before_entries = self._entries(before_tree)
            after_entries = self._entries(after_tree)
            current_entries = self._entries(current_tree)
            for name in set(before_entries) | set(after_entries):
                before_entry = before_entries.get(name)
                after_entry = after_entries.get(name)
                if before_entry == after_entry:
                    continue
                if current_entries.get(name) != after_entry:
                    conflicts.append(name)
                    continue
                if before_entry is None:
                    dags.pop(name, None)
                    tags.pop(name, None)
                else:
                    dags[name], before_tags = before_entry
                    if before_tags:
                        tags[name] = before_tags
                    else:
                        tags.pop(name, None)
            if conflicts:
                raise DmlRepoError(f"Revert conflicts: {sorted(set(conflicts))}")
            new_tree = txn.put(Tree(dags=dags, tags=tags))
            new_commit = txn.put(
                Commit(
                    parents=[base_commit],
                    tree=new_tree,
                    author=user,
                    message=message or f"Revert {target_commit.id()[:8]}",
                    created=created,
                )
            )
            return new_commit

        return db.write_with_growth(revert_commit)

    def rebase(
        self,
        source,
        target,
        user: str,
        *,
        db: DmlDB,
        missing_commits: set[Ref] | None = None,
    ):
        created = now()

        def rebase_commits(txn: TxnWithValid):
            c0 = self._merge_base(source, target, txn=txn, missing_commits=missing_commits)
            if c0 == source:
                return target
            if c0 == target:
                return source
            rebased_parent = target
            for commit_ref in self._linear_path(
                c0, source, txn=txn, missing_commits=missing_commits
            ):
                commit: Commit = txn.get(commit_ref)
                old_parent = commit.parents[0]
                old_tree = self._get_commit(old_parent, txn=txn, missing_commits=missing_commits).tree
                target_tree = txn.get(rebased_parent).tree
                diff = self._diff(old_tree, commit.tree, txn=txn)
                old_entries = self._entries(txn.get(old_tree))
                target_entries = self._entries(txn.get(target_tree))
                conflicts = {
                    name
                    for name in set(diff["add"]) | set(diff["rem"])
                    if target_entries.get(name) != old_entries.get(name)
                    and target_entries.get(name) != diff["add"].get(name)
                }
                if conflicts:
                    raise DmlRepoError(f"Rebase conflicts: {sorted(conflicts)}")
                new_tree = self._patch(
                    target_tree,
                    diff,
                    txn=txn,
                )
                rebased_parent = txn.put(
                    Commit(
                        parents=[rebased_parent],
                        tree=new_tree,
                        author=user,
                        message=commit.message,
                        created=created,
                    )
                )
            return rebased_parent

        return db.write_with_growth(rebase_commits)

    ############################################################
    ################ DAG CHECKOUT AND MANAGEMENT ##############$
    ############################################################
    def checkout_dag(self, commit: Ref | None, dag: Ref, name: str, user: str, db: DmlDB) -> Ref:
        created = now()

        def checkout(txn: TxnWithValid) -> Ref:
            if dag.ns() != "dag":
                raise DmlRepoError(f"Input '{dag.to}' is not a DAG ref")
            if commit is None:
                tree = Tree(dags={name: dag}, tags={})
                parents = []
            else:
                tree = cast(Tree, txn.get(txn.get(commit).tree))
                parents = [commit]
            if name in tree.dags:
                logger.warning(f"DAG name '{name}' already exists in commit; it will be overwritten")
            tree.dags[name] = dag
            tree.tags.pop(name, None)
            new_commit = txn.put(
                Commit(
                    parents=parents,
                    tree=txn.put(tree),
                    author=user,
                    message=f"Checkout DAG '{dag.to}' as '{name}'",
                    created=created,
                )
            )
            return new_commit

        return db.write_with_growth(checkout)

    def delete_dag(self, commit: Ref, name: str, user: str, *, db: DmlDB) -> Ref:
        def delete(txn: TxnWithValid) -> Ref:
            ctx = txn.get_ctx(commit)
            if name not in ctx.tree.dags:
                raise DmlRepoError(f"DAG '{name}' not found in branch commit tree")
            ctx.tree.dags = {k: v for k, v in ctx.tree.dags.items() if k != name}
            ctx.tree.tags = {k: v for k, v in ctx.tree.tags.items() if k != name}
            ctx.commit.tree = txn.put(ctx.tree)
            ctx.commit.author = user
            ctx.commit.parents = [commit]
            ctx.commit.message = f"Delete DAG '{name}'"
            new_commit_ref = txn.put(ctx.commit)
            return new_commit_ref

        return db.write_with_growth(delete)

    def _update_dag_tag(self, commit: Ref, name: str, tag: str, user: str, *, add: bool, db: DmlDB) -> Ref:
        action = "Add" if add else "Remove"
        created = now()

        def update(txn: TxnWithValid) -> Ref:
            ctx = txn.get_ctx(commit)
            if name not in ctx.tree.dags:
                raise DmlRepoError(f"DAG '{name}' not found in branch commit tree")
            tags = list(ctx.tree.tags.get(name, []))
            if add:
                if tag in tags:
                    return commit
                tags.append(tag)
            else:
                if tag not in tags:
                    return commit
                tags.remove(tag)
            if tags:
                ctx.tree.tags[name] = tags
            else:
                ctx.tree.tags.pop(name, None)
            return txn.put(
                Commit(
                    parents=[commit],
                    tree=txn.put(ctx.tree),
                    author=user,
                    message=f"{action} tag '{tag}' for DAG '{name}'",
                    created=created,
                )
            )

        return db.write_with_growth(update)

    def add_dag_tag(self, commit: Ref, name: str, tag: str, user: str, *, db: DmlDB) -> Ref:
        return self._update_dag_tag(commit, name, tag, user, add=True, db=db)

    def remove_dag_tag(self, commit: Ref, name: str, tag: str, user: str, *, db: DmlDB) -> Ref:
        return self._update_dag_tag(commit, name, tag, user, add=False, db=db)

    def get_dag(self, commit: Ref, name: str, *, db: DmlDB) -> Optional[Ref]:
        with db.tx(readonly=True) as txn:
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
                "tags": ctx.tree.tags,
                "author": ctx.commit.author,
                "message": ctx.commit.message,
                "created": ctx.commit.created,
            },
        )

    def log(self, commit: Ref, *, limit: int = 100, db: DmlDB) -> list[CommitDescription]:
        return self.log_with_truncation(commit, limit=limit, db=db)[0]

    def log_with_truncation(
        self,
        commit: Ref,
        *,
        limit: int = 100,
        db: DmlDB,
        missing_commits: set[Ref] | None = None,
    ) -> tuple[list[CommitDescription], bool]:
        to_walk = [commit]
        out = []
        seen: set[Ref] = set()
        truncated = False
        with db.tx(readonly=True) as txn:
            while to_walk and len(out) < limit:
                current = to_walk.pop(0)
                if current in seen:
                    continue
                seen.add(current)
                if not txn.exists(current) and current in (missing_commits or set()):
                    truncated = True
                    continue
                commit_obj = self._get_commit(current, txn=txn, missing_commits=missing_commits)
                out.append(self._describe(current, txn))
                to_walk.extend(commit_obj.parents)
        return out, truncated

    def describe(self, commit: Ref, *, db: DmlDB) -> CommitDescription:
        with db.tx(readonly=True) as txn:
            return self._describe(commit, txn)

    def ahead_behind(
        self,
        local: Ref,
        upstream: Ref,
        *,
        db: DmlDB,
        missing_commits: set[Ref] | None = None,
    ) -> tuple[int, int]:
        if local == upstream:
            return 0, 0
        with db.tx(readonly=True) as txn:
            local_reachable = self._reachable(local, txn=txn, missing_commits=missing_commits)
            upstream_reachable = self._reachable(upstream, txn=txn, missing_commits=missing_commits)
        return len(local_reachable - upstream_reachable), len(upstream_reachable - local_reachable)
