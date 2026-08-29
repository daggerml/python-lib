## 1. Public API Refactor

- [x] 1.1 Replace dashboard imports from `daggerml._core` submodules with permitted public facade imports.
- [x] 1.2 Replace direct configuration resolution and private `Dml` fields with public `Dml.from_config_vars()`, configuration, revision, runtime, DAG, branch, tag, and dependency operations.
- [x] 1.3 Rework bounded history traversal, ref-source grouping, and ref relation calculation as dashboard-owned composition of public `Dml` responses.
- [x] 1.4 Replace internal-base serialization with explicit public DaggerML value serialization while retaining existing redaction and bounds.
- [x] 1.5 Add static boundary tests that fail on dashboard imports from a `daggerml._core` submodule or private `Dml` attribute access.

## 2. CloudWatch-Only Logs

- [x] 2.1 Remove the local executor log reader and every local-log fallback from dashboard detail and event routes.
- [x] 2.2 Derive CloudWatch stream cache identity exclusively from trusted selected execution and function-DAG state for reads and SSE streams.
- [x] 2.3 Return bounded unavailable diagnostics for absent cache keys, missing CloudWatch configuration, missing streams, and CloudWatch failures.
- [x] 2.4 Update log contract tests to prove canonical CloudWatch-only reads and the absence of local file access.

## 3. Persisted Runnable Evidence

- [x] 3.1 Remove Docker, Batch, CloudFormation, PID, and other executor-specific live-resource probe code and its dashboard integrations.
- [x] 3.2 Project public persisted launch state as bounded, redacted, non-authoritative runnable evidence.
- [x] 3.3 Update API response contracts and frontend inspector labels to remove live Resources semantics and present Runnable or launch-state evidence.
- [x] 3.4 Add dashboard contract tests for present and unavailable launch state without executor probing.

## 4. Documentation And Verification

- [x] 4.1 Update dashboard architecture and security documentation for public-API-only reads, CloudWatch-only logs, and the removal of executor probes.
- [x] 4.2 Update affected dashboard, API contract, and frontend tests for the narrowed observability behavior.
- [x] 4.3 Run the repository's required formatting, lint, type-check, and relevant non-slow test suites.
