## Purpose
Define the documentation path for contributors developing DaggerML itself.

## Requirements

### Requirement: Contributor documentation SHALL provide a Develop DaggerML path
The documentation SHALL provide a Develop DaggerML path for contributors changing DaggerML itself, including development setup, testing, codebase orientation, and stable architecture documentation.

#### Scenario: New contributor enters the docs
- **WHEN** a contributor needs to change DaggerML core code
- **THEN** the Develop DaggerML path identifies the contribution setup, testing guidance, codebase map, and relevant architecture material

### Requirement: Contributor documentation SHALL remain distinct from product-user learning paths
The Develop DaggerML path SHALL be visually and structurally separate from Use and Extend paths so that implementation internals do not become required reading for researchers or integration engineers.

#### Scenario: Researcher enters docs home
- **WHEN** a researcher selects Use DaggerML
- **THEN** contributor architecture and repository workflow material are not part of the researcher learning sequence

### Requirement: Contributor documentation SHALL exclude automated maintenance policy
The Develop DaggerML path SHALL not duplicate agent instructions, OpenSpec governance, edit maps, or other automated maintenance policy; those materials SHALL remain outside `docs/`.

#### Scenario: Contributor needs OpenSpec workflow policy
- **WHEN** a contributor needs change-planning or agent workflow instructions
- **THEN** the docs direct the contributor to the appropriate maintainer-oriented material outside `docs/`
