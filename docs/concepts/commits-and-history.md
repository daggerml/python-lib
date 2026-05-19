# Commits and history

DaggerML versions repositories in a git-like way, but the things being versioned are named DAG snapshots.

## The main objects

The history model centers on four pieces:

- `Commit`: an immutable snapshot with parents, metadata, and a tree ref
- `Tree`: a mapping from DAG names to DAG refs
- branch refs: pointers to commits
- index refs: mutable working pointers rooted from a commit

The tree is the bridge between history and DAGs. A commit does not store every DAG inline; it points at a tree, and the tree names the DAGs visible at that revision.

## Working state versus recorded history

When you start new work, DaggerML creates an index from a base commit. That index is where DAG-building operations happen. When you commit, DaggerML writes a new immutable commit and updates either:

- the current branch pointer, or
- a detached output path for function-driven work when no branch update is requested

This separation explains why repository operations feel familiar to source control while still supporting execution-oriented workflows.

## Branches and HEAD

`HeadOps` manages a HEAD file plus branch and index pointers under `.dml/refs/`.

- An attached HEAD follows a branch.
- A detached HEAD points directly at a commit.
- Branch pointers move forward explicitly.
- Index pointers track mutable staging state separately from branches.

In practice, branches answer "which commit is current?" while indexes answer "where is the work in progress?"

## History shape

Commits store parent refs, so ancestry defines history. Merge commits can have two parents. Rebase and merge operate by comparing and rewriting tree state, especially the mapping from DAG names to DAG refs.

The interesting unit of change is usually not a single node. It is which named DAGs a commit's tree adds, removes, or replaces.

## How to think about it

If DAGs are the computation artifacts, commits are the repository timeline that organizes them. The usual reading flow is:

1. resolve a branch or revision to a commit
2. read the commit's tree
3. look up the named DAG you care about
4. inspect the DAG and its nodes

See also:

- [DAGs and nodes](dags-and-nodes.md)
- [Refs and namespaces](refs-and-namespaces.md)
- [Remotes](remotes.md)
