## MODIFIED Requirements

### Requirement: Project docs SHALL use a minimal primary navigation
Primary navigation SHALL contain Start here, Use, and Extend sections. Glossary and Sharp bits and security SHALL be direct top-level siblings of those sections. It SHALL NOT contain separate Concepts, Guides, Reference, Examples, or Develop sections.

#### Scenario: Reader looks for onboarding
- **WHEN** a new researcher wants the fastest path to first success
- **THEN** the docs home directs them into the ordered Start here course

#### Scenario: Reader looks for detailed information
- **WHEN** a reader needs to use DaggerML or implement an integration
- **THEN** the docs navigation directs them to Use or Extend without first requiring them to classify the page as a concept, guide, reference, or example

### Requirement: Documentation source paths SHALL mirror published paths
Human-facing QMD source paths SHALL match their published HTML paths. Start here pages SHALL live directly beneath `docs/start-here/`, researcher pages directly beneath `docs/use/`, and extension pages directly beneath `docs/extend/`. Directory landing pages SHALL use `index.qmd`. Glossary and Sharp bits and security SHALL remain top-level QMD siblings. A separate documentation examples source or route SHALL NOT exist.

#### Scenario: Maintainer locates a published page
- **WHEN** a maintainer maps a `/docs/<section>/<page>` route back to its QMD source
- **THEN** the corresponding page is found at `docs/<section>/<page>.qmd`

#### Scenario: Reader opens the documentation root
- **WHEN** a reader opens `/docs`
- **THEN** the dashboard presents `docs/start-here/index.qmd` without requiring a duplicate root-level QMD

### Requirement: Existing technical content SHALL be preserved through translation, not path churn
When current docs are flattened, the implementation SHALL preserve useful reader-facing technical knowledge in an appropriate Use or Extend page rather than deleting depth solely because its former concept, guide, reference, example, or Develop category is removed. Contributor-only orientation SHALL move to the repository README nearest the code it describes.

#### Scenario: Existing detailed doc is migrated
- **WHEN** a current technical document contains useful reader-facing explanation
- **THEN** the flat documentation preserves that information in Use or Extend even if the page name and presentation change

### Requirement: Maintainer workflow docs SHALL leave `docs/`
Repository-maintenance documents, contributor setup and testing policy, codebase orientation, architecture notes, edit maps, agent instructions, and spec-governance indexes SHALL live outside the human-facing `docs/` tree. Repository-wide contributor guidance SHALL live in root maintainer files, subsystem orientation SHALL live in co-located README files, and normative behavior SHALL remain in OpenSpec.

#### Scenario: Contributor needs workflow guidance
- **WHEN** a contributor needs setup, testing, edit workflow, agent, or spec-governance guidance
- **THEN** that guidance is available from repository-level maintainer files outside `docs/`

#### Scenario: Contributor needs subsystem orientation
- **WHEN** a contributor needs implementation architecture or a codebase map
- **THEN** they can find it in a README co-located with the relevant source area

## ADDED Requirements

### Requirement: Human-facing documentation SHALL be the example surface
Reader-facing examples SHALL appear in the Start here, Use, or Extend page that teaches the behavior. Runnable examples SHALL execute during the documentation build and fail the build on unexpected errors. The documentation SHALL NOT maintain a separate Examples section, parallel canonical example source, or example-only download inventory.

#### Scenario: Reader follows documented behavior
- **WHEN** a page demonstrates a runnable DaggerML workflow
- **THEN** the demonstrated code is part of that page and has been verified by the documentation build

#### Scenario: Maintainer changes an example
- **WHEN** a maintainer changes runnable code shown in the documentation
- **THEN** there is no separate reader-facing example copy that must be updated in parallel

### Requirement: Documentation build entrypoint SHALL live with documentation
The repository SHALL provide `docs/build.sh` as the single local, CI, and release entrypoint for building executable documentation and packaged dashboard assets. A duplicate root-level dashboard build entrypoint SHALL NOT remain.

#### Scenario: Maintainer builds packaged documentation
- **WHEN** a maintainer or automation invokes the documented build command
- **THEN** `docs/build.sh` executes the complete documentation and dashboard asset build

## REMOVED Requirements

### Requirement: Concept documentation SHALL be prose with explicit pseudocode
**Reason**: The documentation no longer classifies pages as concepts or prohibits execution based on directory category; pages may combine explanation with verified examples.

**Migration**: Preserve useful explanatory prose in the relevant flat Use or Extend page and execute runnable examples wherever practical.
