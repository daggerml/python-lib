## Why

Executor workflows make AWS control-plane and scratch-S3 requests that can be throttled or temporarily unavailable. Although the shared Batch client already has an adaptive retry configuration, direct `boto3.client(...)` calls bypass it, and callers cannot tailor timeouts, retry budgets, or connection pools to workload needs.

## What Changes

- Extend the shared AWS client factory with optional connection timeout, read timeout, retry, and maximum connection settings.
- Route all project AWS client creation through the shared factory so every service call receives a consistent baseline client configuration.
- Configure the Batch executor's Batch and scratch-S3 clients with a high-resilience policy suitable for asynchronous job launch and polling.
- Add focused contract tests for client configuration propagation and executor client selection.

## Capabilities

### New Capabilities
- `aws-client-resilience`: Configurable, consistent AWS SDK client construction and high-resilience Batch executor AWS access.

### Modified Capabilities

- None.

## Impact

- Affected code: `daggerml.util.get_client`, contrib executors and adapters, and any core or contrib module that directly calls `boto3.client`.
- Public API: `get_client` gains optional configuration parameters while preserving its current defaults.
- Dependencies: continues to use the existing `boto3` and `botocore` dependencies.
