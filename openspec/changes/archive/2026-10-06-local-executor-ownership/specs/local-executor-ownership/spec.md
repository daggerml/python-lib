## Purpose

Define safe continuation and resource teardown for script and Docker executions shared by callers on different machines, without changing transparent transports or remotely addressable executors.

## ADDED Requirements

### Requirement: Local launch state SHALL identify its owning execution environment

Script and Docker SHALL include a required nonempty string `owner` in the adapter state returned when launching detached work. Owner identity SHALL be stable across fresh adapter processes in the same execution environment. Participating host/container environments SHALL use distinct owner identities. Backend launch fields and other continuation fields SHALL remain preserved. Ownership SHALL NOT change runnable identity or cache identity. Saved object state without `owner` SHALL be unsupported; no compatibility fallback SHALL infer or claim ownership.

#### Scenario: Launch identifies the environment performing the work
- **WHEN** script or Docker starts detached work on environment A
- **THEN** its returned adapter state contains owner A and the executor's backend launch fields

#### Scenario: Fresh process retains the same ownership identity
- **WHEN** another adapter process on environment A receives A-owned launch state
- **THEN** it identifies itself as the same owner and can continue the existing work

#### Scenario: Containerized script identifies its own environment
- **WHEN** script launches inside a Docker container
- **THEN** script returns the container environment's owner identity using the same launch contract as direct script execution
- **AND** that identity is distinct from the host's Docker owner identity

### Requirement: Ownership SHALL distinguish effective users on the same host

The owner identifier SHALL have format `<effective UID>@<hostname>`, resolved in the execution environment where the executor runs. Different effective users on the same hostname SHALL have different owner identifiers. Username environment variables SHALL NOT determine identity. Ownerless or hostname-only saved state SHALL NOT receive a compatibility fallback.

#### Scenario: Different users share a host
- **WHEN** two executor processes run on the same hostname with different effective UIDs
- **THEN** they produce different owner identifiers
- **AND** neither can pass the other's local-resource ownership check

#### Scenario: Username environment variables do not change ownership
- **WHEN** the username environment changes without changing effective UID or hostname
- **THEN** the owner identifier remains unchanged

### Requirement: Local resource operations SHALL require matching ownership

For saved object state, script and Docker SHALL check its required owner before any local resource inspection or teardown during invoke continuation, cleanup, or cancel. A different owner SHALL return status `retry`, unchanged object `adapter_state`, and null error without inspecting or modifying local processes, files, containers, or images. It SHALL NOT replace ownership, launch substitute work, report completed cleanup, or confirm cancellation. Matching ownership SHALL retain existing backend behavior. Null-state cleanup and cancellation SHALL retain their existing no-resource behavior.

#### Scenario: Different machine polls shared state
- **WHEN** environment B receives A-owned script or Docker state for an invoke continuation
- **THEN** it returns retry with that state unchanged
- **AND** it performs no local resource access

#### Scenario: Different machine attempts published-result cleanup
- **WHEN** environment B receives A-owned script or Docker state for cleanup
- **THEN** it returns retry without deleting resources or reporting cleanup success
- **AND** it preserves all continuation fields

#### Scenario: Different machine attempts cancellation
- **WHEN** environment B receives A-owned script or Docker state for cancel
- **THEN** it returns retry without signaling a process or stopping/removing a container
- **AND** it does not return cancelled

#### Scenario: Owning environment continues local operations
- **WHEN** environment A receives A-owned state for continuation, cleanup, or cancellation
- **THEN** the existing backend operation proceeds against A's resources

### Requirement: Transparent transports SHALL NOT pin ownership to their caller

SSH SHALL forward operations and nested adapter state unchanged to its configured destination without adding a caller owner. Local ownership restrictions SHALL apply where script or Docker actually executes, not to the entire runnable chain. Batch SHALL NOT acquire a local-machine ownership restriction.

#### Scenario: Two callers reach the same owning SSH destination
- **WHEN** A and B call an SSH-wrapped Docker or script execution on C with C-owned saved state
- **THEN** each forwards that state to C
- **AND** the nested executor on C accepts its matching ownership

#### Scenario: Batch continuation is independent of the original caller
- **WHEN** another caller checks an existing Batch execution using its saved job state
- **THEN** no script/Docker local-owner restriction is imposed on the Batch executor
