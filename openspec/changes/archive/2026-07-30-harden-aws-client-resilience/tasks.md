## 1. Shared AWS Client Configuration

- [x] 1.1 Extend `daggerml.util.get_client` with keyword-only connection timeout, read timeout, retry attempts, retry mode, and maximum connection-pool overrides while preserving the existing defaults and region resolution.
- [x] 1.2 Add unit coverage proving default and override values are passed to the botocore client configuration.

## 2. Centralize Production Client Creation

- [x] 2.1 Replace direct `boto3.client(...)` construction in production source with `get_client(...)`, preserving each caller's service and existing non-default pool settings.
- [x] 2.2 Update affected test seams and add regression coverage that executor and adapter AWS operations use the shared factory.

## 3. Batch Resilience Policy

- [x] 3.1 Define the Batch executor's 60-second connect/read timeout, 10-attempt adaptive retry, and 100-connection pool policy.
- [x] 3.2 Route Batch control-plane and scratch-S3 reads/writes through clients configured with that policy.
- [x] 3.3 Add focused tests covering the high-resilience configuration on Batch launch and poll paths.

## 4. Verification

- [x] 4.1 Run formatting and the targeted util, contrib adapter, and executor test suites.
- [x] 4.2 Run the full test suite and resolve any client-factory migration regressions.
