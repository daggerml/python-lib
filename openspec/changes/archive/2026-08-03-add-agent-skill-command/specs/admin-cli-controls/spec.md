## ADDED Requirements

### Requirement: Admin exports the bundled agent skill
`dml admin agent-skill` SHALL write the complete bundled agent skill document to standard output and SHALL not write command framing or serialized representation around that document.

#### Scenario: User redirects the agent skill to a file
- **WHEN** a user runs `dml admin agent-skill > SKILL.md`
- **THEN** `SKILL.md` contains the complete bundled agent skill document
- **AND** it begins with the skill document's YAML frontmatter
