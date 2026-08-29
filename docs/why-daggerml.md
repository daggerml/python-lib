# Why DaggerML?

DaggerML makes a computation a durable research artifact: its inputs, functions, results, execution boundaries, and provenance are recorded as a DAG. Re-running the same work can use a known result; inspecting a result can lead back to the computation that produced it.

It is a good fit when research needs repeatable derived results, reusable intermediate work, auditable inputs and outputs, or execution beyond one local Python process. It combines Python authoring with a CLI for project history, configuration, remotes, and runtime operations.

It is not a general workflow scheduler, a replacement for object storage, or a promise that arbitrary Python state can be replayed. Large data normally remains in external storage and is represented in a DAG by an artifact URI.

Start with [Use DaggerML](use/README.md), or review the shared [glossary](glossary.md).
