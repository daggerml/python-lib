## Purpose
Define documentation for engineers building DaggerML extensions and integrations.

## Requirements

### Requirement: Extension documentation SHALL serve integration engineers
The documentation SHALL provide an ordered Extend course containing exactly three primary pages named Codecs, Adapters, and Executors. Codecs SHALL combine custom value-conversion concepts, contracts, implementation, registration, packaging, built-ins, and only the narrow `ProjectionCodec` lowering mechanism; user-facing `Projection` inspection remains in Use. Adapters SHALL distinguish the core adapter contract—any CLI executable available on `PATH` or by a fully specified path that implements the JSON stdin/stdout protocol—from optional contrib `AdapterBase` hooks for resolution, transport, and CLI exposure. Adapters SHALL explain delayed authoring and lowering, logical adapter discovery, direct concrete `Runnable` construction, optional delegation to contrib executors, operation payloads, registration, and deployment. Executors SHALL be presented as a contrib-only abstraction, not discovered or imported by core, combining delegated runnable construction and resolution, backend lifecycle, durable continuation, result and cleanup ownership, nesting, remote concerns, built-ins, registration, and deployment. The course SHALL not retain separate concept, guide, plugin, packaging, testing, built-in-inventory, or reference pages that duplicate those journeys. Published pages SHALL NOT include test or verification checklists; executable example validation SHALL remain part of the documentation build.

#### Scenario: Engineer implements an execution integration
- **WHEN** an integration engineer needs to write an adapter or executor
- **THEN** the applicable Extend page explains and demonstrates the model, lifecycle, and implementation, distinguishes core executable dispatch from optional contrib hooks and executors, and validates runnable examples during the build without publishing a testing checklist

#### Scenario: Engineer implements data conversion
- **WHEN** an integration engineer follows Codecs
- **THEN** one page explains and demonstrates the codec contract, recursive normalization, registration, packaging, and built-in behavior with build-validated examples

#### Scenario: Engineer changes ProjectionCodec mechanics
- **WHEN** an integration engineer changes `ProjectionCodec` lowering internals
- **THEN** Codecs explains that narrow mechanism without taking ownership of researcher-facing Projection traversal

#### Scenario: Engineer implements a transport integration
- **WHEN** an integration engineer follows Adapters
- **THEN** one page defines the core CLI executable boundary on `PATH` or at a fully specified path, demonstrates transport operations, and explains delayed authoring, direct `Runnable` construction, and optional contrib `AdapterBase` hooks and executor delegation

#### Scenario: Engineer implements backend behavior
- **WHEN** an integration engineer follows Executors
- **THEN** one page explains and demonstrates contrib-only delegated backend-specific runnable resolution, lifecycle dispatch, state, idempotency, cancellation, cleanup, nesting, and deployment without implying core discovers or imports executors

#### Scenario: Engineer follows the course
- **WHEN** the three Extend pages are displayed or executed
- **THEN** they appear and run in the declared Codecs, Adapters, Executors order

### Requirement: Extension documentation SHALL separate extension contracts from researcher workflows
The Extend course SHALL own codec contracts, core adapter executable operations, optional contrib adapter hooks and executor lifecycle contracts, plugin registration, package and executable deployment, and provider-authoring details. Use SHALL own operation of installed capabilities. Cross-cutting extension concerns SHALL appear in the mechanism page where a reader applies them, while dashboard-provider contracts that do not fit codecs, adapters, or executors SHALL move to dashboard integration documentation rather than remain as a generic plugin page. Contributor testing procedures and verification checklists SHALL remain outside published course pages.

#### Scenario: Researcher follows a built-in integration page
- **WHEN** a researcher uses a supported execution capability
- **THEN** its Use page does not require adapter protocol knowledge

#### Scenario: Engineer needs protocol details
- **WHEN** an integration engineer follows the corresponding Extend link
- **THEN** that page defines and demonstrates the relevant public extension contract and lifecycle semantics

#### Scenario: Researcher follows a built-in integration
- **WHEN** a researcher uses a supported execution or data capability
- **THEN** its Use page does not require adapter, executor, codec-registry, or provider-schema implementation knowledge

#### Scenario: Engineer needs registration or packaging details
- **WHEN** an integration engineer implements a codec, adapter, or executor
- **THEN** the corresponding Extend page includes the applicable entry-point, executable, installation-topology, and diagnostic guidance

#### Scenario: Engineer implements a dashboard provider
- **WHEN** an integration engineer needs dashboard registration and result-schema contracts
- **THEN** dashboard integration documentation provides those contracts outside the three-page codec, adapter, and executor course

### Requirement: Extension documentation SHALL not use contrib as its primary navigation category
The documentation SHALL organize extension content by integration goals rather than the `daggerml.contrib` package name, while retaining exact import paths and package names where reference material requires them.

#### Scenario: Engineer enters extension docs
- **WHEN** an integration engineer seeks adapter or codec guidance
- **THEN** the primary navigation presents Extend DaggerML rather than requiring the reader to infer that the material is under contrib

### Requirement: Extension examples SHALL be executable documentation
Runnable codec, adapter, executor, plugin, and packaging examples SHALL live in the applicable Codecs, Adapters, or Executors page and SHALL be verified by the documentation build. The three pages SHALL form a declared dependency sequence and MAY share only durable workspace, installed-package, project, file, or object-storage state across page boundaries. Examples requiring external infrastructure not owned by the documentation fixture SHALL be explicitly marked as pseudocode and paired with executable contract coverage where practical. The Extend path SHALL NOT depend on a separate example download or example-only page hierarchy.

#### Scenario: Extension example changes
- **WHEN** a maintainer changes runnable extension guidance
- **THEN** the documentation build executes the changed example and fails if it no longer satisfies the documented behavior

#### Scenario: Extend pages share course state
- **WHEN** a later Extend page consumes an integration package or fixture produced earlier
- **THEN** the prerequisite is declared and the consumed state is persisted outside an earlier page's interpreter memory

#### Scenario: Integration needs unavailable infrastructure
- **WHEN** an example requires a real transport or backend not controlled by the documentation build
- **THEN** the page distinguishes pseudocode from verified examples and does not require ambient credentials or services
