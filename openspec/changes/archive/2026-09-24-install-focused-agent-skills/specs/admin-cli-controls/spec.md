## MODIFIED Requirements

### Requirement: Admin exports the bundled agent skill
The generated CLI SHALL expose `dml skills querying`, `dml skills authoring`, `dml skills repository`, and `dml skills extensions`. Each command SHALL accept a required destination parent directory, an optional `--name` (defaulting to `daggerml-<kind>`), and an optional `--overwrite`. It SHALL install the corresponding bundled skill directory at `<parent>/<name>/`, including `SKILL.md` and supporting files, with frontmatter `name` matching the directory name, and return a diagnostic identifying the directory and total bytes written. It SHALL reject unsafe names, and SHALL fail without modifying an existing target unless `--overwrite` is passed. The CLI SHALL NOT expose `dml skills inspection`, `dml admin agent-skill`, or an `admin` namespace.

#### Scenario: Install a focused skill
- **WHEN** a user runs `dml skills authoring .opencode/skills`
- **THEN** `.opencode/skills/daggerml-authoring/SKILL.md` contains the complete authoring skill
- **AND** the command reports its directory and total amount written

#### Scenario: Install authoring example
- **WHEN** a user runs `dml skills authoring .opencode/skills`
- **THEN** `.opencode/skills/daggerml-authoring/examples/dagclass.py` is installed alongside `SKILL.md`

#### Scenario: Install with a custom name
- **WHEN** a user runs `dml skills querying .agents/skills --name research-querying`
- **THEN** `.agents/skills/research-querying/SKILL.md` has `name: research-querying` in its frontmatter

#### Scenario: Existing target requires explicit replacement
- **WHEN** a destination skill directory already exists and `--overwrite` is absent
- **THEN** installation fails without modifying that directory
- **WHEN** `--overwrite` is present
- **THEN** its bundled files are replaced and unrelated contents of that directory are preserved

#### Scenario: Invalid skill name
- **WHEN** a name includes a path separator, traversal, or is not a valid skill directory name
- **THEN** installation fails without writing outside the destination parent

#### Scenario: Replaced inspection route is rejected
- **WHEN** a user runs `dml skills inspection`
- **THEN** command parsing fails because `inspection` is not a generated skill command

#### Scenario: Removed administrative route is rejected
- **WHEN** a user runs `dml admin agent-skill`
- **THEN** command parsing fails because `admin` is not a generated CLI namespace
