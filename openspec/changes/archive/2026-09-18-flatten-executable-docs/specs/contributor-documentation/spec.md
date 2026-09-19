## ADDED Requirements

### Requirement: Contributor documentation SHALL live with repository maintenance and source
Contributor setup, testing, and contribution policy SHALL live in repository-level maintainer files outside `docs/`. Stable subsystem and codebase orientation SHALL live in README files co-located with the code they describe. Normative behavioral contracts SHALL remain in OpenSpec.

#### Scenario: Contributor prepares a checkout
- **WHEN** a contributor needs setup, test, lint, or contribution instructions
- **THEN** the root repository guidance provides them without a Develop section in human-facing product docs

#### Scenario: Contributor changes a subsystem
- **WHEN** a contributor needs implementation orientation for a source area
- **THEN** the nearest applicable README describes that area and directs them to authoritative specs where necessary

## REMOVED Requirements

### Requirement: Contributor documentation SHALL provide a Develop DaggerML path
**Reason**: Human-facing product docs are limited to Start here, Use, and Extend; contributor guidance belongs beside repository maintenance and source.

**Migration**: Move setup and testing guidance to root contributor files and stable subsystem orientation to co-located READMEs.

### Requirement: Contributor documentation SHALL remain distinct from product-user learning paths
**Reason**: Contributor material is no longer a documentation path, so separation is enforced by keeping it outside `docs/`.

**Migration**: Route contributors through root maintainer files and source READMEs rather than dashboard Docs navigation.

### Requirement: Contributor documentation SHALL exclude automated maintenance policy
**Reason**: The complete contributor surface now lives outside human-facing product docs, making a Develop-specific exclusion obsolete.

**Migration**: Keep agent instructions, OpenSpec governance, edit maps, and contributor policy in their existing repository-level locations.
