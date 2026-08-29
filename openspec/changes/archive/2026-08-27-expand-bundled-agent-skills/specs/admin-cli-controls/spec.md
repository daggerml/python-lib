## MODIFIED Requirements

### Requirement: Admin exports the bundled agent skill
The generated CLI SHALL expose `dml skills querying`, `dml skills authoring`, `dml skills repository`, and `dml skills extensions`. Each command SHALL write its complete corresponding bundled skill document to standard output and SHALL not write command framing or serialized representation around that document. The CLI SHALL NOT expose `dml skills inspection`, `dml admin agent-skill`, or an `admin` namespace.

#### Scenario: User redirects a focused skill to a file
- **WHEN** a user runs `dml skills authoring > SKILL.md`
- **THEN** `SKILL.md` contains the complete bundled `authoring` skill document
- **AND** it begins with the skill document's YAML frontmatter

#### Scenario: User exports each focused skill
- **WHEN** a user runs each of `dml skills querying`, `dml skills authoring`, `dml skills repository`, and `dml skills extensions`
- **THEN** each command succeeds and prints only its corresponding bundled document

#### Scenario: Replaced inspection route is rejected
- **WHEN** a user runs `dml skills inspection`
- **THEN** command parsing fails because `inspection` is not a generated skill command

#### Scenario: Removed administrative route is rejected
- **WHEN** a user runs `dml admin agent-skill`
- **THEN** command parsing fails because `admin` is not a generated CLI namespace
