## MODIFIED Requirements

### Requirement: `Dml` delegates repository behavior to the relevant ops classes
The shared `Dml` class SHALL orchestrate workflows by delegating repository actions to the relevant subsystem ops classes rather than re-implementing those mechanics inline. Module-level helper functions in `daggerml._internal.dml` SHALL construct the owning concrete ops classes directly and SHALL NOT route calls through a facade object or string-dispatch proxy layer.

#### Scenario: Commit-oriented workflow delegates to CommitOps
- **WHEN** a caller invokes `dml.show`, `dml.log`, `dml.diff`, `dml.merge`, or `dml.revert`
- **THEN** `Dml` delegates the relevant repository operations to `CommitOps` after preparing resolved inputs

#### Scenario: Runtime workflow delegates to IndexOps
- **WHEN** a caller invokes `dml.runtime.create`, `dml.runtime.put_literal`, `dml.runtime.start_fn`, or `dml.runtime.commit`
- **THEN** `Dml` delegates the relevant repository operations to `IndexOps` after preparing resolved inputs

#### Scenario: Admin workflow delegates to the owning subsystem
- **WHEN** a caller invokes an admin cache, remote, or gc workflow
- **THEN** `Dml` delegates the repository action to `CacheOps`, `RemoteOps`, or `GcOps` respectively after preparing resolved inputs

#### Scenario: Helper construction instantiates concrete ops directly
- **WHEN** a shared `Dml` workflow needs an ops object such as `CommitOps`, `HeadOps`, `IndexOps`, or `RemoteOps`
- **THEN** the helper logic in `daggerml._internal.dml` constructs that concrete ops class directly against the active DB handle
- **AND** it does not dispatch through a `DmlOps` facade or `_OpsProxy`-style string factory
