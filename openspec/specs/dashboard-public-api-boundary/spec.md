# dashboard-public-api-boundary Specification

## Purpose

Define the dashboard's dependency and observability boundary around the public
DaggerML API, trusted persisted state, and canonical CloudWatch logs.

## Requirements

### Requirement: Dashboard core access uses only public facades
The dashboard SHALL import DaggerML core values only from the public
`daggerml._core` facade, `daggerml`, or `daggerml.contrib`. It SHALL NOT import
from a `daggerml._core` submodule, access a private `Dml` attribute, or duplicate
repository storage semantics. Dashboard repository, revision, ref, runtime, and
execution reads SHALL use the existing public `Dml` API and dashboard-owned,
bounded projections.

#### Scenario: Dashboard inspects repository state
- **WHEN** the dashboard resolves a revision, traverses history, compares refs,
  lists live indexes, or reads execution lineage
- **THEN** it uses public `Dml` operations and bounded dashboard composition
- **AND** it does not access core storage objects, repository-head objects, or
  private fields

#### Scenario: Dashboard serializes persisted values
- **WHEN** a dashboard response contains a public DaggerML value
- **THEN** the dashboard serializes explicit public value types and ordinary
  mappings and sequences under its existing redaction and response bounds
- **AND** it does not depend on an internal common base type

### Requirement: Dashboard logs are CloudWatch-only
The dashboard SHALL read and stream only the canonical CloudWatch `dml` log
group streams named `/run/{trusted-cache-key}/{stdout|stderr}`. The trusted cache
key SHALL be derived from revision-reachable persisted DAG or execution state;
the browser SHALL NOT supply it directly. The dashboard SHALL NOT read local
executor log files or any other local log source.

#### Scenario: Trusted execution has a CloudWatch log stream
- **WHEN** a selected execution or function DAG provides a trusted persisted
  cache key and the requested stream is stdout or stderr
- **THEN** the dashboard reads or streams the corresponding canonical CloudWatch
  stream within its existing bounds

#### Scenario: CloudWatch logs are unavailable
- **WHEN** the selected resource has no trusted cache key, CloudWatch is
  unconfigured, a stream is absent, or CloudWatch access fails
- **THEN** the dashboard reports a bounded logs-unavailable state
- **AND** it does not fall back to a local executor log path

### Requirement: Dashboard does not probe live executor resources
The dashboard SHALL present persisted runnable configuration and bounded,
redacted persisted launch state as non-authoritative evidence. It SHALL NOT
inspect local processes, Docker containers, AWS Batch jobs, CloudFormation
stacks, or other executor-specific live resources.

#### Scenario: Researcher inspects a runnable execution
- **WHEN** a selected runnable has persisted launch state
- **THEN** the dashboard presents that state as bounded, redacted JSON alongside
  the persisted runnable chain
- **AND** it does not add interpreted live status from an executor-specific
  probe

#### Scenario: Persisted launch state is unavailable
- **WHEN** no launch state is recorded or the runtime state cannot be read
- **THEN** the dashboard presents a bounded availability diagnostic
- **AND** it does not attempt an executor-specific probe as a fallback
