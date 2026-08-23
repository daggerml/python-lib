## Purpose
Define centralized and resilient AWS client construction for runtime and executor operations.

## Requirements

### Requirement: Configurable AWS client construction
The system SHALL provide `get_client` as the production AWS client construction path. It SHALL resolve the AWS region using its existing precedence and SHALL accept keyword options for connection timeout, read timeout, retry attempts, retry mode, and maximum pooled connections. When an option is omitted, it SHALL preserve the existing default client policy.

#### Scenario: Caller overrides the client policy
- **WHEN** a caller provides connection timeout, read timeout, retry attempts, retry mode, and maximum pooled connections to `get_client`
- **THEN** the resulting boto3 client SHALL receive a botocore configuration containing those values and the resolved region

#### Scenario: Caller uses default client policy
- **WHEN** a caller invokes `get_client` without client-policy overrides
- **THEN** the resulting client SHALL use a 5-second connection timeout, adaptive retries with five attempts, and 20 maximum pooled connections

### Requirement: Centralized production AWS clients
The system SHALL construct AWS clients in production source code through `get_client` rather than direct `boto3.client(...)` calls.

#### Scenario: Executor scratch object access
- **WHEN** an executor reads or writes an S3 scratch object
- **THEN** it SHALL use an S3 client obtained through `get_client`

#### Scenario: Adapter AWS access
- **WHEN** an adapter performs an AWS service operation
- **THEN** it SHALL use a service client obtained through `get_client`

### Requirement: High-resilience Batch executor clients
The Batch executor SHALL use a high-resilience client policy for all Batch control-plane and scratch-S3 requests. Its clients SHALL use a 60-second connection timeout, 60-second read timeout, adaptive retry mode, and 100 maximum pooled connections. Launch requests SHALL use 100 retry attempts; polling, output-read, and cancellation requests SHALL use 25 retry attempts.

#### Scenario: Batch job launch
- **WHEN** the Batch executor writes its input or registers and submits a Batch job
- **THEN** each AWS client used by those operations SHALL use 100 retry attempts

#### Scenario: Batch job polling
- **WHEN** the Batch executor describes a job or reads its scratch output
- **THEN** each AWS client used by those operations SHALL use 25 retry attempts
