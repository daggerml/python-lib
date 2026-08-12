## Why

Dagclass methods currently retain delayed references that resolve against the DAG where the method is later staged. Caller nodes can therefore shadow dagclass attributes, so a supposedly configured, reusable dagclass method changes behavior according to ambient caller names.

## What Changes

- Compile each dagclass instance into a self-contained namespace when it is instantiated.
- Initialize the namespace from evaluated dagclass attributes, then bind topologically ordered members into that namespace as compilation proceeds.
- Resolve every member `api.ref(name)` against the partially built dagclass namespace and reject references to names not yet available there.
- Make compiled entrypoints and other exported members independent of names in the calling DAG.
- Document the sharp edge that an externally defined funk adopted as a dagclass member may reference only names known within that dagclass namespace.
- Keep `api.run()` focused on executing the already compiled entrypoint rather than performing compilation.

## Capabilities

### New Capabilities

- `dagclass-namespace-compilation`: Defines instantiation-time dagclass compilation, namespace-scoped delayed references, compilation failures, and execution of compiled entrypoints.

### Modified Capabilities

None.

## Impact

- Affected API: `daggerml.contrib.api.dagclass`, direct access to compiled dagclass members, and `daggerml.contrib.api.run`.
- Affected internals: dagclass member discovery, dependency analysis, topological compilation, delayed-reference binding, and nested runnable traversal in `src/daggerml/contrib/api.py`.
- Affected tests: contrib contract tests for instance compilation, direct method staging, caller-name collisions, external funks, invalid references, cycles, nested members, and `api.run()`.
- Affected documentation: dagclass composition and `api.ref` scoping guidance.
- No new dependencies.
