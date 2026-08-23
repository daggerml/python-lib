## MODIFIED Requirements

### Requirement: Canonical-path registry deduplicates same-process DB access
The DB layer SHALL canonicalize each requested DB path and use a process-local registry so all callers targeting the same canonical path reuse the same registry slot. The supported boundary SHALL describe acquisition by DB facades and operations without exposing a separate compatibility lifecycle for raw handles.

#### Scenario: Same-path callers reuse one slot
- **WHEN** two DB facades acquire environments in the same process for paths that canonicalize to the same on-disk DB location
- **THEN** the DB layer assigns both acquisitions to the same registry slot

#### Scenario: Different paths use different slots
- **WHEN** two DB facades acquire environments in the same process for paths that canonicalize to different on-disk DB locations
- **THEN** the DB layer assigns them to different registry slots

### Requirement: Registry invalidates inherited state on PID change
The DB layer SHALL store the active PID on the registry and clear all inherited registry environments before further acquisition when the PID does not match the current process. It SHALL NOT expose handle-reopen aliases or handle-level fork errors as an alternate recovery path.

#### Scenario: Child process clears inherited registry state
- **WHEN** a process fork occurs and the child attempts to acquire a DB environment through the inherited registry
- **THEN** the child clears the inherited registry state and continues with a fresh registry PID

### Requirement: Registry capacity is bounded and explicit
The DB layer SHALL enforce a fixed maximum number of distinct canonical DB paths in the registry at one time and fail with a dedicated registry-capacity error when no slot is available.

#### Scenario: Registry-full returns a dedicated error
- **WHEN** a caller acquires a DB environment for a new canonical path and all registry slots are already occupied by different paths
- **THEN** the DB layer fails with a dedicated registry-capacity error
