## 1. Bundled Skill Resource

- [x] 1.1 Create the concise, portable `SKILL.md` package resource with `name` and `description` YAML frontmatter, compact shell/Python examples, DAG/node guidance, funk worker constraints, sharp-bit examples, remote guidance, and managed-project boundaries grounded in the current documentation.
- [x] 1.2 Configure package data so the skill resource is included in wheel and sdist builds and can be read with package-resource APIs.

## 2. CLI Export Surface

- [x] 2.1 Add the zero-argument `agent_skill` command to the Dml administration namespace, loading and returning the bundled resource unchanged so generated CLI discovery exposes `dml admin agent-skill`.
- [x] 2.2 Add the agent-skill command to the CLI reference with its redirection-oriented usage.

## 3. Verification

- [x] 3.1 Add CLI contract tests for `dml admin agent-skill`, including its generated help entry, successful output, raw frontmatter, and output equivalence with the bundled resource.
- [x] 3.2 Add resource-content tests that enforce the required portable metadata and concise DaggerML guidance.
- [x] 3.3 Build a wheel and verify an installed distribution includes the resource and serves the same command output.
- [x] 3.4 Run the required formatter, type checks, and focused non-slow tests.
