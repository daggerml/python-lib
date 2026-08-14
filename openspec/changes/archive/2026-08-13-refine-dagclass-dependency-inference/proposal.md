## Why

Dagclass compilation currently treats `self.<name>` through flow-sensitive member analysis without accounting for attributes that Python resolves directly on the worker `Dag`. This rejects valid calls such as `self.put(...)`, prevents methods from creating named nodes with `self.foo = value`, and risks building a topology that does not match runtime attribute lookup.

## What Changes

- Define dagclass method dependencies as non-reserved `self.<name>` attribute accesses, with one graph edge per referenced dagclass member.
- Exclude every name that resolves as an attribute on the worker `daggerml.api.Dag`, because Python resolves those names before `Dag.__getattr__` can perform named-node access.
- Remove a name's dependency edge when the method contains any assignment to `self.<name>`, regardless of control flow or source ordering, and permit such assignments without a class-level member declaration.
- Keep item access such as `self["name"]` completely outside compilation analysis; document prominently that it neither creates nor removes topology edges.
- Validate that both endpoints of every inferred dependency edge belong to the dagclass member collection.
- Replace flow-sensitive definite-assignment behavior with conservative syntactic collection; compilation will not evaluate reachability, ordering, or control flow.
- Document reserved-name collisions, assignment assumptions, invocation-local named nodes, item-access limitations, and resulting runtime failure modes as sharp bits.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `dagclass-namespace-compilation`: Define syntactic dependency-edge inference, `Dag`-resolved reserved names, method-wide assignment exclusion, edge validation, and the item-access analysis boundary.

## Impact

- Affected compiler: `src/daggerml/contrib/api.py` dagclass AST analysis and topology construction.
- Affected tests: dagclass compiler contracts and integration coverage under `tests/contrib/`.
- Affected documentation: `docs/use/guides/author-a-dag.md` and the dedicated `docs/sharp-bits-and-security.md` warning surface.
- Public behavior changes for dagclass methods that use `self.<name>`, especially `Dag` operations, assignments, control-flow-dependent reads, and item access.
- No new runtime dependency or storage-format change.
