# Overview

DaggerML combines a few ideas that fit together closely:

- A DAG is an immutable record of one computation.
- Nodes are the individual values, imports, and function calls inside that DAG.
- Commits version named DAGs the same way a source-control system versions files.
- Refs are the typed identities that connect every persisted object.
- Execution turns a runnable call into another DAG, often through adapters and remote cache state.
- Storage keeps local repository objects in a content-addressed object store and leaves large external payloads behind URIs.
- Remotes publish commits, DAGs, and cache results into an S3-backed CAS-plus-refs layout.
- Codecs normalize Python values into the stored value model before they enter a DAG.

One useful way to read the system is from outside in:

1. You create or load a repository runtime with `Dml`.
2. You open a working DAG through `new()` or `Dml.new(...)`.
3. As you assign values or call functions, DaggerML stages nodes into an index.
4. Finishing the work produces a DAG snapshot.
5. Committing records that snapshot in history and attaches it to a branch or detached commit.
6. Optional remote sync publishes the resulting state to shared storage.

That split is important: the working index is mutable, but the DAGs and commits it produces are not.

Read next:

- [DAGs and nodes](dags-and-nodes.md) for the graph model.
- [Commits and history](commits-and-history.md) for repository versioning.
- [Execution](execution.md) for what happens during function calls.
