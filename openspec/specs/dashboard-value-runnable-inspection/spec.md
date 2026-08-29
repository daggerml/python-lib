# dashboard-value-runnable-inspection Specification

## Purpose

Define distinct, addressable dashboard inspector surfaces for every node's persisted value and for the runnable applied by a function context, with safe shared runnable, script, and prepopulation presentation.

## Requirements

### Requirement: Every node has an addressable Value tab
The dashboard SHALL provide a Value tab for every selected node, SHALL preserve that tab in the inspector query state, and SHALL move the node's bounded persisted-value presentation out of Summary.

#### Scenario: Inspect a non-Runnable node value
- **WHEN** a user selects a node whose persisted value is not a Runnable and opens Value
- **THEN** the dashboard shows a bounded preview appropriate to that value without runnable-specific sections

#### Scenario: Inspect an unavailable or error value
- **WHEN** the selected node's value cannot be materialized normally or represents an error
- **THEN** Value shows the bounded error or availability evidence rather than removing the tab

#### Scenario: Restore a Value deep link
- **WHEN** a node inspector route is restored with `tab=value`
- **THEN** the dashboard selects Value for that node without rewriting the owning project, revision, DAG, or node scope

### Requirement: Runnable node values use the shared runnable presentation in Value
The read model SHALL identify Runnable values explicitly without requiring the browser to infer their type from object shape. When a node's persisted value is a Runnable, Value SHALL render that value using the same runnable presentation structure used by the function-applied Runnable tab while retaining the Value tab name and node-value meaning.

#### Scenario: Ordinary node contains a Runnable value
- **WHEN** a non-FnNode's persisted value is a Runnable
- **THEN** its Value tab shows the runnable stack, entrypoint, script evidence, and prepopulation information and the inspector does not add a function-applied Runnable tab

#### Scenario: FnNode returns a Runnable
- **WHEN** an FnNode's persisted return value is a Runnable
- **THEN** Value shows the returned Runnable independently from the Runnable tab that shows the function-applied runnable

### Requirement: Runnable tab is limited to function application context
The dashboard SHALL provide a Runnable tab only for an FnNode and for the function-context DAG referenced by an FnNode. That tab SHALL describe the function-applied runnable represented by the context DAG's first argument, `argv[0]`.

#### Scenario: Inspect an FnNode Runnable
- **WHEN** a user opens Runnable for an FnNode
- **THEN** the dashboard shows the runnable applied to produce that node and does not substitute the node's returned value

#### Scenario: Inspect a function-context DAG Runnable
- **WHEN** a user opens Runnable for a DAG that is a persisted function context
- **THEN** the dashboard shows the same function-applied runnable represented by that DAG's `argv[0]`

#### Scenario: Inspect another node or DAG
- **WHEN** the selected resource is neither an FnNode nor a function-context DAG
- **THEN** the inspector does not offer the function-applied Runnable tab

#### Scenario: Restore a Runnable deep link
- **WHEN** an FnNode or function-context DAG inspector route is restored with `tab=runnable`
- **THEN** the dashboard selects Runnable without rewriting the owning project, revision, DAG, node, or context scope

### Requirement: Function-applied runnable identity is exact
The read model SHALL derive the function-applied runnable from `argv[0]` of the persisted context DAG and SHALL NOT select a runnable merely because it occurs elsewhere in the argument value, in prepopulation, or in another nested container.

#### Scenario: Prepopulation contains another Runnable
- **WHEN** `argv[0]` contains or is accompanied by Runnable values in prepopulation or other arguments
- **THEN** Runnable identifies only `argv[0]` as the function-applied root and treats the others as nested values rather than execution-stack layers

#### Scenario: Function context lacks a Runnable first argument
- **WHEN** a purported function context has no first argument or `argv[0]` is not a Runnable
- **THEN** the read model reports bounded unavailable evidence and does not invent an applied runnable

### Requirement: Runnable presentation preserves stack order and roles
The shared runnable presentation SHALL show the outermost runnable followed by each successive `sub` runnable through the innermost entrypoint. Each layer SHALL expose its safe target, adapter, kind, and pertinent executor configuration, subject to response bounds and redaction.

#### Scenario: Wrapped script runnable
- **WHEN** an SSH, Docker, Batch, or other supported wrapper contains a script runnable through `sub`
- **THEN** the dashboard shows the wrapper before the script and identifies the script as the innermost entrypoint

#### Scenario: Runnable stack reaches its bound
- **WHEN** the persisted `sub` chain exceeds the dashboard's runnable-depth bound
- **THEN** the dashboard marks the stack as truncated and performs no unbounded traversal

### Requirement: Script source is derived safely from the innermost runnable
The dashboard SHALL show bounded Python source only when the innermost runnable is a script executor with a persisted Python `script_uri`. Function-applied source reads SHALL derive the URI from the trusted function-context DAG, and Runnable-value source reads SHALL derive it from the trusted, revision-reachable node. Neither interface SHALL accept an arbitrary script URI from the browser.

#### Scenario: Function-applied entrypoint is a readable Python script
- **WHEN** the innermost applied runnable is a script executor with a trusted readable Python `script_uri`
- **THEN** Runnable shows the sanitized URI and bounded source with a truncation indicator when applicable

#### Scenario: Runnable value entrypoint is a readable Python script
- **WHEN** the innermost Runnable stored as a node value is a script executor with a trusted readable Python `script_uri`
- **THEN** Value shows the sanitized URI and bounded source obtained from that node's scoped script link

#### Scenario: Innermost runnable is not a Python script
- **WHEN** the innermost runnable is not a script executor
- **THEN** the shared presentation explains that Python source is unavailable because the entrypoint is another runnable kind

#### Scenario: Script metadata or access is unavailable
- **WHEN** the innermost runnable lacks `script_uri`, the remote is unconfigured, the URI is outside the configured trusted root, access is denied, or the persisted object cannot be read
- **THEN** the shared presentation shows a cause-specific bounded explanation and does not silently omit the Script section

#### Scenario: Node script request is outside revision scope
- **WHEN** a client requests Runnable-value script source for a node not reachable from the selected revision or eligible current live DAG
- **THEN** the API rejects the request without reading the supplied node as another revision's resource and without falling back to HEAD

### Requirement: Script prepopulation is presented as names, types, and scoped links
For a script entrypoint, the shared runnable presentation SHALL list each prepopulation entry by name and safe value type. It SHALL link to a node only when a corresponding node exists and is valid in the inspected applied context, and SHALL explicitly indicate when the entry has not been instantiated or no scoped link is available.

#### Scenario: Applied script has committed prepopulated nodes
- **WHEN** a function-applied script's prepopulation names correspond to nodes in its persisted context DAG
- **THEN** Runnable shows each name, its value type, and a link that opens the corresponding node in the same project and revision scope

#### Scenario: Returned Runnable declares uninstantiated prepopulation
- **WHEN** a Runnable stored as a node value declares prepopulation that has not been applied in a function context
- **THEN** Value shows each name and value type and marks its node link as not instantiated

#### Scenario: Prepopulation contains sensitive or large values
- **WHEN** prepopulation values contain credentials, environment values, large payloads, or deeply nested objects
- **THEN** the dashboard returns only the bounded redacted name/type/link projection and does not expose the raw prepopulation value

### Requirement: Summary does not duplicate Value or Runnable details
Summary SHALL retain concise identity, status, properties, and context navigation while the Value and Runnable tabs own their respective value preview and runnable stack, script, and prepopulation content.

#### Scenario: Inspect an FnNode Summary
- **WHEN** an FnNode has both a persisted value and function-applied runnable evidence
- **THEN** Summary does not duplicate either detailed presentation and provides navigation to the dedicated tabs and context DAG as applicable

### Requirement: Runnable evidence excludes executor-local resource inspection
The shared runnable presentation SHALL retain the persisted runnable stack and
safe executor configuration. Where persisted execution launch state is available,
it SHALL expose that state only as bounded, redacted, non-authoritative JSON.
The presentation SHALL NOT inspect or display local executor logs, process
status, Docker container state, Batch job state, CloudFormation state, or other
executor-specific live resource probes.

#### Scenario: Runnable has persisted launch state
- **WHEN** a user inspects a Runnable whose execution has persisted launch state
- **THEN** the dashboard presents bounded redacted launch-state JSON with the
  runnable evidence
- **AND** it does not infer executor lifecycle or resource health from that JSON

#### Scenario: Runnable has no launch state
- **WHEN** a user inspects a Runnable without available persisted launch state
- **THEN** the dashboard presents bounded unavailable evidence
- **AND** it does not probe an executor or host for substitute status
