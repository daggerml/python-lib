## Why

The suite has strong focused contracts and several real integrations, but major public workflows are not proven end to end across repository, runtime, CLI, and installed-package boundaries. A lifecycle suite is needed to ensure every supported capability has an explicit integration or external-acceptance owner.

## What Changes

- Add a maintained capability-to-lifecycle coverage matrix covering every public surface and recording its deterministic or external acceptance owner.
- Add deterministic slow scenarios using real LMDB, Moto S3, subprocesses, and Docker where available for repository synchronization, shallow history, dependencies, runtime/cache, garbage collection, installed CLI, dashboard launcher, and installed plugin workflows.
- Extend existing lifecycle suites where the real system boundary is already established instead of duplicating focused contract coverage.
- Define separately gated external acceptance scenarios for SSH and AWS Lambda, Batch, and S3 behavior that cannot be verified faithfully with local fixtures.
- Keep fast contract tests as the owner of exact invalid states, protocol matrices, and race injection.
- Record public tag publication and shallow-tip `Dml.show` inspection as deferred product-behavior gaps when lifecycle tests expose them; their expected failures are not passing coverage. Fixing those behaviors requires a separate change.

## Capabilities

### New Capabilities

None. This adds verification coverage without changing user-visible product requirements.

### Modified Capabilities

None.

## Impact

- Slow test suites under `tests/`, test fixtures, CI test selection, and contributor testing guidance.
- Docker-capable and installed-wheel test environments.
- Optional, credential-gated external acceptance infrastructure for SSH and AWS services.
- No public API, storage format, or runtime behavior changes.
