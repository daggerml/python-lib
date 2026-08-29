## Context

The shared configuration model already defines canonical parameter names such as `project.home`, `remote.uri`, and `config_home`, and the CLI delegates config resolution to `DmlConfig`. Even so, the top-level parser and some help/error surfaces still expose older frontend-specific spellings like `--repo` and `--remote-root`. The change is cross-cutting because it touches parser setup, help examples, normalized error hints, and CLI-focused tests across multiple command paths.

The concrete CLI files affected by the audit are `src/daggerml/_cli/__init__.py`, `base.py`, `init.py`, `status.py`, `config.py`, and `remote.py`. Other CLI modules do not currently hardcode the legacy flag names, so they are expected to remain unchanged unless verification uncovers stale help text.

## Goals / Non-Goals

**Goals:**
- Rename user-facing CLI flags so explicit overrides mirror the canonical config keys they feed.
- Keep the CLI thin by changing parser names and forwarded argument attributes without moving domain logic into CLI modules.
- Update help text, examples, and structured error hints so the docs and runtime guidance use one naming scheme.
- Update tests so contract coverage reflects the renamed public surface.

**Non-Goals:**
- Changing the underlying config schema, resolution precedence, or environment variable names.
- Adding compatibility aliases for the old flag spellings.
- Renaming Python API arguments such as `Dml(repo=...)` as part of this change.

## Decisions

### Rename only CLI-facing flags, not canonical internal fields
The implementation will rename parser options like `--repo` to `--project-home` and `--remote-root` to `--remote-uri`, while continuing to resolve those values through `DmlConfig` as `project.home` and `remote.uri`. This keeps the internal contract unchanged and limits the change to the transport surface.

Alternative considered: rename internal config fields or add a second normalization layer in CLI code. That was rejected because the canonical internal names are already established in docs and code, and extra translation logic would weaken the thin-interface contract.

### Treat the rename as an intentional breaking CLI update
The old spellings will be removed rather than kept as aliases. This matches the request to make CLI flags replicas of config args throughout and avoids indefinite dual-name maintenance in help text, tests, and user guidance.

Alternative considered: keep both old and new flags for a deprecation period. That was rejected because it would preserve the naming ambiguity this change is trying to remove.

### Preserve handler wiring and output behavior
Handlers will continue to forward parsed values into the same internal operations, with attribute names adjusted as needed to avoid business logic changes. Success payloads and structured error formatting stay the same except for user-facing hints that mention the renamed flags.

Alternative considered: broaden the change into CLI argument refactoring or parser restructuring. That was rejected because the minimal correct change is a surface rename plus matching documentation and test updates.

### Separate parser destinations where public flag names overlap
The top-level CLI should expose `--remote-uri`, and `init` should continue exposing its own `--remote-uri` input for project bootstrap. The implementation will keep the shared public spelling but use distinct argparse destinations so command execution can distinguish top-level runtime override input from init-specific remote configuration input.

Alternative considered: rename one of the two public flags to avoid overlap. That was rejected because `remote.uri` is already the canonical config name for both concepts, and the requested change is to make CLI flags mirror config argument names throughout.

## Risks / Trade-offs

- [Existing scripts break on old flags] -> Mitigation: document the rename as breaking in the proposal/specs and update all in-repo examples/tests in the same change.
- [Some help text or error hints keep stale names] -> Mitigation: update shared helper messages and grep CLI/tests/docs for `--repo` and `--remote-root` before concluding the implementation.
- [Parser dest renames could accidentally break command execution] -> Mitigation: keep the forwarding shape explicit in each handler and verify through CLI contract/integration tests.
- [Top-level and init `--remote-uri` values collide in the argparse namespace] -> Mitigation: assign separate internal destinations and add parser/command tests that cover both forms.

## Migration Plan

1. Rename the top-level and command-specific parser flags to canonical config-shaped names in `__init__.py`, `init.py`, `status.py`, and `remote.py`.
2. Update CLI handlers and shared helpers in `base.py`, `init.py`, `status.py`, and `config.py` to read the new parsed argument attributes.
3. Assign separate argparse destinations for the top-level and init `--remote-uri` flags.
4. Update help examples, docs, and normalized error hints.
5. Update CLI contract and integration tests to use the new flag names and ensure stale names are rejected.

Rollback is straightforward: restore the prior parser option names and corresponding examples/messages if the renamed surface causes unacceptable breakage before release.

## Open Questions

- None. The requested direction is explicit: CLI flags should match the config argument names throughout.
