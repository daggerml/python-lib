## Why

Remote branch pushes currently overwrite the branch ref without checking whether the local commit advances the remote tip. Concurrent or divergent clients can therefore silently replace a remote branch head and hide another client's commits.

## What Changes

- Add `force: bool = False` to `Dml.push()` for explicit non-fast-forward branch replacement.
- Make ordinary branch pushes read and materialize the current remote tip without updating local heads or working state.
- Reject an ordinary branch push unless the observed remote tip is an ancestor of the commit being published.
- Publish new remote branches with a create-only write and existing remote branches with an ETag-conditional write, preventing check-then-write races.
- Keep ordinary tag publication create-only while allowing force pushes to explicitly overwrite branches or tags.
- Keep `Dml` as an API wrapper by placing commit ancestry and remote ref publication behavior in their owning operation modules.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `remote-project-refs`: Define safe default branch-push, force-push, remote-tip materialization, and conditional publication behavior.

## Impact

- Affected code: `src/daggerml/_core/dml.py`, `src/daggerml/_core/remote.py`, `src/daggerml/_core/commit.py`, and remote sync tests.
- Public API: `Dml.push()` gains a keyword-only `force` option.
- Remote behavior: normal branch updates become fast-forward-only and race-safe; non-forced tag publication remains create-only.
- No new dependencies or remote layout changes.
