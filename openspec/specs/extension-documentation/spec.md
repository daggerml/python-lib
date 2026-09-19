## Purpose
Define documentation for engineers building DaggerML extensions and integrations.

## Requirements

### Requirement: Extension documentation SHALL serve integration engineers
The documentation SHALL provide a flat Extend path for readers who implement adapters, executors, codecs, plugin registrations, remote integrations, and supporting infrastructure. Each topic SHALL combine the explanation, public contract details, implementation guidance, executable examples, and testing guidance needed for that extension without requiring concept, guide, or reference subdirectories.

#### Scenario: Engineer implements an execution integration
- **WHEN** an integration engineer needs to write an adapter or executor
- **THEN** a directly addressable Extend page explains and demonstrates the required model, lifecycle, implementation, and tests

#### Scenario: Engineer implements data conversion
- **WHEN** an integration engineer needs to write a codec
- **THEN** a directly addressable Extend page explains and demonstrates the codec contract, registration, and verification workflow

### Requirement: Extension documentation SHALL separate extension contracts from researcher workflows
The flat Extend path SHALL document adapter operations, executor lifecycle contracts, plugin registration, and shared-codec contracts without making those details prerequisites for researchers who use supported integrations. Use pages SHALL link to the relevant Extend page only when a reader needs to implement or customize the integration.

#### Scenario: Researcher follows a built-in integration page
- **WHEN** a researcher uses a supported execution capability
- **THEN** its Use page does not require adapter protocol knowledge

#### Scenario: Engineer needs protocol details
- **WHEN** an integration engineer follows the corresponding Extend link
- **THEN** that page defines and demonstrates the relevant public extension contract and lifecycle semantics

### Requirement: Extension documentation SHALL not use contrib as its primary navigation category
The documentation SHALL organize extension content by integration goals rather than the `daggerml.contrib` package name, while retaining exact import paths and package names where reference material requires them.

#### Scenario: Engineer enters extension docs
- **WHEN** an integration engineer seeks adapter or codec guidance
- **THEN** the primary navigation presents Extend DaggerML rather than requiring the reader to infer that the material is under contrib

### Requirement: Extension examples SHALL be executable documentation
Runnable adapter, executor, codec, plugin, packaging, and integration-testing examples SHALL live in their relevant Extend pages and SHALL be verified by the documentation build. The Extend path SHALL NOT depend on a separate example download or example-only page hierarchy.

#### Scenario: Extension example changes
- **WHEN** a maintainer changes runnable extension guidance
- **THEN** the documentation build executes the changed example and fails if it no longer satisfies the documented behavior
