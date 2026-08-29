## Context

See proposal.md for motivation. The generated CLI discovers public `Dml` properties and methods. Today the sole `_AdminNamespace.agent_skill()` loads one package-level `SKILL.md`; its content mixes workflows and the CLI exposes it as `dml admin agent-skill`. Exported skills must be useful from a pip-installed package, where repository documentation and examples are not assumed to be present. The public namespace and CLI contracts are owned by `unified-dml-surface` and `admin-cli-controls`.

## Goals / Non-Goals

**Goals:**
- Provide three compact, independently exportable Markdown skills for authoring, repository operations, and DAG/runtime inspection.
- Make the public Python and generated CLI surfaces accurately classify these resources as skills.
- Make installed source locations discoverable for readers who need deeper implementation detail.

**Non-Goals:**
- Add a fourth umbrella skill, agent-specific runtime behavior, or dynamic skill selection.
- Change DAG, cache, repository, runtime, or generated-CLI mechanics beyond the namespace and commands needed to export resources.
- Preserve `admin` or `agent-skill` compatibility routes during v0.

## Decisions

### Store one resource per skill under `_core/skills`

Place `authoring.md`, `repository.md`, and `inspection.md` in `src/daggerml/_core/skills/` and include them as package data. The skill export surface reads a named resource unchanged apart from the existing terminal-newline handling needed by the CLI string serializer.

This makes each resource reviewable and independently portable. A single templated document with sections was rejected because it retains unrelated guidance in every export; generated content was rejected because skills are authored documents, not runtime data.

### Expose a terminal `skills` namespace

Replace `_AdminNamespace` and `Dml.admin` with a terminal `_SkillsNamespace` and `Dml.skills`. Its zero-argument `authoring()`, `repository()`, and `inspection()` methods load the respective resources, so method discovery produces `dml skills <name>` directly.

No alias, compatibility property, or legacy command is retained. This fits the v0 no-backward-compatibility constraint and avoids maintaining duplicate generated help paths.

### Keep skill scope exclusive and concise

`authoring` owns construction, script execution boundaries, normalized cache identity, and provenance-preserving inputs. `repository` owns revision and remote lifecycle. `inspection` owns immutable and mutable graph states, traversal, errors, execution lineage, and cache diagnosis. Each includes only a minimal example where syntax has high error risk, and contains the operational guidance necessary without repository documentation. A skill can name installed source modules for deeper investigation, but must not link to or require repository documentation or examples.

This division ensures an agent requesting one operational task does not receive the other two. Replacing the current generic skill with one larger rewritten document was rejected for the same reason.

### Update canonical and validation surfaces together

Update package data, user-facing CLI/Python docs, contract tests, and OpenSpec deltas in the same implementation. Resource-output tests compare each command's bytes to its package resource and enforce compact, self-contained topic-specific guidance without documentation or example links. CLI namespace tests assert `skills` exists and `admin` is absent.

## Risks / Trade-offs

- [Skill content drifts from runtime behavior] -> Keep prose compact, point only to installed source modules for deeper investigation, and test essential guidance phrases and resource-output equivalence.
- [Resource files are omitted from built distributions] -> Add package-data coverage and test resource loading from the installed package path.
- [Generated CLI help silently changes with public property discovery] -> Extend help and route-rejection contract tests for both `skills` and removed `admin`.
- [Focused skill omits important context] -> Keep cross-cutting details as links rather than duplicating content or widening every skill.

## Migration Plan

1. Add package resources and the `skills` namespace with its three exports.
2. Remove `admin` and the legacy `admin agent-skill` generated route in the same patch.
3. Update documentation and contracts to name only `dml skills <name>`.
4. Release as a v0 breaking change; no runtime migration, data migration, or rollback compatibility path is required.
