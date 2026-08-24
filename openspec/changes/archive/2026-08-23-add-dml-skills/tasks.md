## 1. Bundled Skill Resources

- [x] 1.1 Create `src/daggerml/_core/skills/authoring.md` with self-contained, compact DAG-authoring, script-worker, provenance, remote, and cache-identity guidance; use at most two minimal examples and only installed source-code pointers for further detail.
- [x] 1.2 Create `src/daggerml/_core/skills/repository.md` with self-contained, compact Git-like history, ref, synchronization, dependency, shallow-history, and safe-GC guidance; use at most one minimal command sequence and only installed source-code pointers for further detail.
- [x] 1.3 Create `src/daggerml/_core/skills/inspection.md` with self-contained, compact committed/frozen/active/execution state, graph traversal, error, runtime-lineage, and cache-diagnosis guidance; use at most one minimal traversal example and only installed source-code pointers for further detail.
- [x] 1.4 Configure package and source-distribution data so all three Markdown resources are available through `importlib.resources` in built distributions.

## 2. Public Export Surface

- [x] 2.1 Replace `_AdminNamespace` and `Dml.admin` with `_SkillsNamespace` and `Dml.skills`, exposing zero-argument `authoring`, `repository`, and `inspection` resource exports.
- [x] 2.2 Remove the package-level generic `SKILL.md` resource and the `agent_skill` export with no Python or CLI compatibility aliases.
- [x] 2.3 Update public Python and CLI reference documentation to describe `Dml.skills` and `dml skills <authoring|repository|inspection>` only.

## 3. Contract Coverage

- [x] 3.1 Replace single-agent-skill contracts with parameterized resource and CLI-output contracts for all three skills, including frontmatter, resource equivalence, concise self-contained topic-specific guidance, minimal examples, and no repository documentation/example links.
- [x] 3.2 Update generated namespace-help and route contracts to assert `skills` and its three commands are present and `admin` plus `admin agent-skill` are rejected.
- [x] 3.3 Run focused API/CLI contract tests and the standard non-slow test and lint checks.
