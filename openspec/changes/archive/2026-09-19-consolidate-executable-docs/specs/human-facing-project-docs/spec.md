## ADDED Requirements

### Requirement: Course navigation SHALL follow validated dependency order
The documentation manifest SHALL expose the validated topological execution position of each page, and dashboard navigation SHALL order the pages within Start here, Use, and Extend by that position. Every non-home course page SHALL declare the prerequisite that establishes its required durable state. Navigation and execution SHALL therefore present the same relative order for each course.

#### Scenario: Reader opens a course section
- **WHEN** the dashboard displays Start here, Use, or Extend
- **THEN** the section's pages appear in the same relative order used to execute their examples

#### Scenario: Maintainer changes a course prerequisite
- **WHEN** a valid `depends-on` relationship changes the topological page order
- **THEN** both documentation execution and dashboard navigation reflect the new order without a separate navigation list

#### Scenario: Course dependencies are invalid
- **WHEN** a page declares an unknown, self-referential, duplicate, or cyclic prerequisite
- **THEN** documentation validation fails before publishing a manifest

### Requirement: Primary course sections SHALL minimize page inventory
Start here, Use, and Extend SHALL expose only substantive course destinations in their dashboard sections. A section SHALL NOT retain a landing, forwarding, concept, guide, reference, example, packaging, testing, or inventory page when its useful content has been consolidated into the ordered course. Removed content SHALL be translated into the owning course page, glossary, Sharp bits and security page, subsystem documentation, or contributor documentation as appropriate, and internal links SHALL target the resulting canonical destination. Published pages SHALL NOT expose administrative metadata, implementation checklists, or test and verification checklists as reader-facing content; executable example assertions and build validation SHALL remain in place.

#### Scenario: Reader opens Use navigation
- **WHEN** the consolidated researcher course is packaged
- **THEN** Use contains Projects, Artifacts, Execution, Inspection, Runtimes, and Sharing without a duplicate Use landing or standalone reference entries

#### Scenario: Reader opens Extend navigation
- **WHEN** the consolidated extension course is packaged
- **THEN** Extend contains Codecs, Adapters, and Executors without a duplicate Extend landing or cross-cutting inventory entries

#### Scenario: Existing technical page is removed
- **WHEN** a superseded source page contains useful technical guidance
- **THEN** that guidance remains discoverable in the audience and course page that owns the workflow

#### Scenario: Removed route is referenced internally
- **WHEN** documentation validation checks links after consolidation
- **THEN** no packaged page links to a removed Use or Extend route

#### Scenario: Reader views a published page
- **WHEN** a course page is published
- **THEN** its reader-facing content omits administrative metadata and implementation, test, and verification checklists while its runnable examples remain build-validated

## MODIFIED Requirements

### Requirement: Human-facing documentation SHALL be the example surface
Reader-facing examples SHALL appear in the Start here, Use, or Extend course page that teaches the behavior. Primary workflows supported by build-owned prerequisites SHALL execute during the documentation build, assert their observable results, and fail the build on unexpected errors. Examples that would require ambient credentials, user projects, or external infrastructure outside the build fixture SHALL be explicitly marked as pseudocode and SHALL not be represented as build-verified. The documentation SHALL NOT maintain a separate Examples section, parallel canonical example source, or example-only download inventory.

#### Scenario: Reader follows documented behavior
- **WHEN** a page demonstrates a runnable DaggerML workflow supported by the documentation fixture
- **THEN** the demonstrated code is part of that page and has been executed with assertions by the documentation build

#### Scenario: Reader views infrastructure-specific guidance
- **WHEN** a workflow requires infrastructure not provisioned by the documentation build
- **THEN** the example is visibly distinguished from executable course code and identifies the missing prerequisite

#### Scenario: Maintainer changes an example
- **WHEN** a maintainer changes runnable code shown in the documentation
- **THEN** there is no separate reader-facing example copy that must be updated in parallel
