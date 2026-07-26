## ADDED Requirements

### Requirement: Extension documentation SHALL serve integration engineers
The documentation SHALL provide an Extend DaggerML path for readers who implement adapters, executors, codecs, plugin registrations, remote integrations, and supporting infrastructure.

#### Scenario: Engineer implements an execution integration
- **WHEN** an integration engineer needs to write an adapter or executor
- **THEN** the Extend DaggerML path provides the required conceptual model, contract reference, implementation guidance, and testing guidance

### Requirement: Extension documentation SHALL separate extension contracts from researcher workflows
The Extend DaggerML path SHALL document adapter operations, executor lifecycle contracts, plugin registration, and shared-codec contracts without making those details prerequisites for researchers who use supported integrations.

#### Scenario: Researcher follows a Docker guide
- **WHEN** a researcher follows documentation for a built-in execution capability
- **THEN** the guide does not require adapter protocol knowledge and links to Extend DaggerML only for implementation details

#### Scenario: Engineer needs protocol details
- **WHEN** an integration engineer opens extension reference material
- **THEN** the material defines the relevant public extension contract and its lifecycle semantics

### Requirement: Extension documentation SHALL not use contrib as its primary navigation category
The documentation SHALL organize extension content by integration goals rather than the `daggerml.contrib` package name, while retaining exact import paths and package names where reference material requires them.

#### Scenario: Engineer enters extension docs
- **WHEN** an integration engineer seeks adapter or codec guidance
- **THEN** the primary navigation presents Extend DaggerML rather than requiring the reader to infer that the material is under contrib
