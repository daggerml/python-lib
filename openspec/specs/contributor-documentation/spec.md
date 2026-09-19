## Purpose
Define the documentation path for contributors developing DaggerML itself.

## Requirements

### Requirement: Contributor documentation SHALL live with repository maintenance and source
Contributor setup, testing, and contribution policy SHALL live in repository-level maintainer files outside `docs/`. Stable subsystem and codebase orientation SHALL live in README files co-located with the code they describe. Normative behavioral contracts SHALL remain in OpenSpec.

#### Scenario: Contributor prepares a checkout
- **WHEN** a contributor needs setup, test, lint, or contribution instructions
- **THEN** the root repository guidance provides them without a Develop section in human-facing product docs

#### Scenario: Contributor changes a subsystem
- **WHEN** a contributor needs implementation orientation for a source area
- **THEN** the nearest applicable README describes that area and directs them to authoritative specs where necessary
