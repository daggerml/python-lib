# Credential-gated acceptance

Run only against disposable infrastructure you control. These tests require
`--run-external -m external` and are never counted as covered by Moto or the
default CI suite. They use an isolated project home and a unique S3 prefix.

S3 artifact/project round trips use Moto, and SSH worker execution/cancellation
uses a disposable local OpenSSH server backed by Moto. Both run in deterministic
CI; neither requires real AWS credentials or a provisioned SSH host. See
`tests/contrib/integration/test_s3_store_integration.py` and
`tests/contrib/integration/test_ssh_lifecycle_integration.py`.

## AWS

For Lambda/Batch deployment acceptance, set `ACCEPTANCE_AWS_BUCKET`,
`ACCEPTANCE_AWS_REGION`,
`ACCEPTANCE_AWS_ACCESS_KEY_ID`, and `ACCEPTANCE_AWS_SECRET_ACCESS_KEY`;
`ACCEPTANCE_AWS_SESSION_TOKEN` is optional. These values must name a disposable
bucket whose prefix may be written/deleted. Set `ACCEPTANCE_LAMBDA_URI`,
`ACCEPTANCE_BATCH_IMAGE`, `ACCEPTANCE_CPU_QUEUE`, and
`ACCEPTANCE_BATCH_TASK_ROLE_ARN` for Batch acceptance. Deploy the Lambda
adapter handler and the matching DaggerML version before running Batch tests;
the image must contain `dml-local-adapter`. Ensure the IAM roles allow S3 object
access, Batch submit/describe/terminate and log reading. Teardown requires
deleting test-owned S3 prefixes, jobs, logs, and any provisioned test resources.

Run: `uv run --dev --all-extras pytest --run-external -m external tests/external/`.
Record the command, environment target, passing, failed, skipped and xfailed
counts in the release report. Unavailable Lambda/Batch credentials are
**unverified**, not a pass. Never run this against a production bucket without
an isolated prefix.
