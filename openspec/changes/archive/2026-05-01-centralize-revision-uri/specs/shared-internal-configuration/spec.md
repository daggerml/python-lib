## MODIFIED Requirements

### Requirement: Project URI is normalized and exposes helper accessors
The system SHALL normalize and canonicalize `remote.project` through shared revision URI utilities. Resolved project configuration MAY target a branch or a tag. The resolved config object SHALL continue to expose helper accessors for the effective project selector.

#### Scenario: Missing selector parses as default branch
- **WHEN** `remote.project` is provided without a branch or tag in `project/runtime` scope
- **THEN** shared revision URI parsing resolves it to a fully realized branch selector using the effective default branch

#### Scenario: Tag URI is accepted for project context
- **WHEN** `remote.project` is provided with a tag selector
- **THEN** project configuration resolution succeeds and preserves canonical tag form

#### Scenario: Project helper accessors derive from canonical URI
- **WHEN** resolved configuration includes `remote.project`
- **THEN** helper accessors derive selector values from canonical parsed URI rather than standalone duplicated parsing logic
