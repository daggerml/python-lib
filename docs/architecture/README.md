# Architecture

This section is for readers who already know what DaggerML does and want to see how the pieces are put together.

- [System overview](system-overview.md): the main layers, data flow, and where public APIs hand work to the internals.
- [Internal modules](internal-modules.md): a map of the main packages and files in `src/daggerml/`.
- [Ops layer](ops-layer.md): the transactional subsystems that implement repo behavior.
- [Storage internals](storage-internals.md): how refs, namespaces, LMDB storage, and on-disk pointers fit together.
- [Remote protocol](remote-protocol.md): how S3-backed CAS, refs, manifests, and execution metadata work.
- [Type system](type-system.md): the dataclasses, namespace registry, and validation rules that shape persisted state.

These pages describe the current implementation. They stay close to the real module layout so contributors can move between the docs and the code without translation.
