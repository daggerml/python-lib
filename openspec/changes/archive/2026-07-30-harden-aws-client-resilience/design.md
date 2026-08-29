## Context

`daggerml.util.get_client` currently resolves an AWS region and builds a botocore `Config` with fixed connection, retry, and pool settings. Several production paths construct clients directly with `boto3.client(...)`, including executor scratch-S3 reads and writes. Those calls therefore omit the shared adaptive retry configuration.

Batch execution crosses several AWS requests: it writes the invocation payload to S3, registers and submits a Batch job, polls Batch, and reads the job output from S3. These control-plane and object-store calls need a more forgiving request policy than ordinary callers. The policy must be supplied to the SDK client, not implemented as a generic executor-method retry, because replaying a launch after an ambiguous response can duplicate external work.

## Goals / Non-Goals

**Goals:**

- Make connection timeout, read timeout, retry budget/mode, and maximum connection-pool size configurable through `get_client`.
- Ensure production AWS clients are created through `get_client`.
- Apply an explicit high-resilience configuration to every Batch executor AWS client, including scratch S3.
- Retain the current default region resolution and default client policy for callers that do not opt in to overrides.

**Non-Goals:**

- Change executor polling cadence or add durable polling backoff.
- Add a generic retry decorator around executor methods or AWS operations.
- Change the behavior of test fixtures or examples whose direct boto3 clients create local test infrastructure.
- Change AWS credentials, endpoint selection, or region-resolution behavior.

## Decisions

### Expose botocore client settings as keyword-only `get_client` options

`get_client` will accept optional `connection_timeout`, `read_timeout`, `max_attempts`, `retry_mode`, and `max_pool_connections` keyword arguments. Omitted values retain the current policy: a 5-second connection timeout, adaptive retry mode, five retry attempts, and 20 pooled connections. The function will translate those values into a single `botocore.config.Config` passed to `boto3.client`.

Keyword-only overrides keep existing positional region arguments compatible and make call-site policy legible. Accepting a raw botocore `Config` was considered, but would leak SDK configuration construction into every caller and make the required common defaults easier to bypass.

### Centralize production client creation without caching clients

Every direct `boto3.client(...)` call under `src/daggerml` will be replaced with `get_client(...)`. Call sites that need a service-specific policy will pass it explicitly.

This change will not introduce a global client cache. Caching could retain stale credentials or test configuration and is unnecessary to ensure that each request receives botocore retry/backoff behavior. It also avoids altering current client lifecycle behavior.

### Give Batch a high-resilience client policy for both services

`BatchExecutor` will define and use a single explicit policy for its Batch and S3 clients: 60-second connection and read timeouts, adaptive retry mode with 10 retry attempts, and 100 maximum pooled connections. Its scratch input/output helpers will receive or construct the S3 client through that policy instead of creating direct boto3 clients.

The values intentionally favor completion of asynchronous remote work over fast failure, while remaining bounded. Applying the policy to both Batch and S3 ensures the request path is consistent: protecting only `register_job_definition`, `submit_job`, and `describe_jobs` would still permit scratch handoff failures to surface directly.

### Preserve exception handling semantics in this change

Existing Batch and CloudFormation polling code has its own broad exception handling. This change only ensures botocore exhausts the configured retry policy before an exception reaches that code; it does not redefine terminal versus retryable executor errors. Separating this avoids silently changing lifecycle behavior while centralizing client construction.

## Risks / Trade-offs

- [Longer Batch request waits delay failure reporting] → Timeouts and retries remain bounded, and the high-resilience policy is limited to Batch executor AWS clients.
- [SDK retries can follow an ambiguous mutating request] → Do not add executor-level replay; retain AWS SDK retry semantics and treat launch idempotency as a separate concern.
- [A missed direct client call bypasses the policy] → Search all production source for `boto3.client` and add regression coverage for the migrated paths.
- [Tests relying on direct client monkeypatches could break] → Update tests to patch the shared factory or inject the executor client at the existing seams.
