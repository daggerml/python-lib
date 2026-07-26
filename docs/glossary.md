# Glossary

- **Project**: a DaggerML repository rooted at `.dml/`, with local history and configuration.
- **DAG**: a durable directed acyclic graph that records a computation and its result.
- **Node**: a value, import, runnable, or function result in a DAG.
- **Result**: the node committed as a DAG's final outcome.
- **Funk**: any DaggerML-packaged `Runnable` object. Once inserted into a DAG, DaggerML knows how to run it. Current authoring tooling is Python-based, but a funk is not inherently Python-specific.
- **Runtime**: mutable, open computation state while a DAG is being authored or inspected. A runtime is finalized into a DAG.
- **Cache**: a remote mapping from normalized DaggerML data that identifies a computation to its completed DAG result. Cache identity is part of the DaggerML data model, not a Python-specific mechanism.
- **Artifact**: external data represented by a `Uri` in a DAG rather than embedded as repository data.
- **Codec**: a conversion from a Python value to a value DaggerML can store.
- **Provenance**: the chain of DAG imports and function calls that produced a value.
- **Remote**: S3-backed storage used for project synchronization and, when needed, distributed execution and cache coordination.
