## MODIFIED Requirements

### Requirement: Manual cancellation SHALL run as one synchronous runtime workflow
The system SHALL treat cancellation as a synchronous execution-control operation keyed by an execution id or index-root id. `Dml.runtime.cancel` SHALL be a thin public entrypoint into `IndexOps.cancel`. One `cancel(this_exec)` call SHALL mutate `this_exec` directly, and any child state changes SHALL occur only inside nested `cancel(child)` runtime calls.

The cancellation algorithm SHALL operate as follows:

1. Resolve `this_exec` as the target execution id; for an index-root cancel, `this_exec` SHALL equal the index id.
2. If `this_exec` is a live index root, atomically move `indexes/<this_exec>.json` to `indexes/.cancelled/<this_exec>.json` under lock unless it is already moved.
3. Read `exec/state/<this_exec>.json`.
4. Acquire the coordination lock for the record's `cache_key`.
5. Delete `active/<cache_key>` if present.
6. Release the coordination lock.
7. Persist `exec/state/<this_exec>.json` with `lifecycle = "cancel-pending"` and `cancellation_requested_by` set to the requesting entity.
8. Read `spawned_execution_ids` from `exec/state/<this_exec>.json`.
9. Process all direct child edges concurrently with a thread pool.
10. For each direct child `callee`, remove `exec/edges/<callee>/<this_exec>.json` if it exists, then invoke the adapter chain responsible for that child once.
11. That adapter chain SHALL own execution and cancellation of the child job. If that child has nested runtime work, the chain SHALL enter `cancel(callee)` at most once for that child execution.
12. Wait for the child worker pool to finish.
13. Ignore child adapter return values for lifecycle purposes.
14. Persist `exec/state/<this_exec>.json` with `lifecycle = "cancelled"`.
15. If `this_exec` is an index root, delete `indexes/.cancelled/<this_exec>.json`.

#### Scenario: Root cancel moves the index pointer first
- **WHEN** a user cancels index `idx1`
- **THEN** the system SHALL move `indexes/idx1.json` to `indexes/.cancelled/idx1.json` before any cancellation fan-out work

#### Scenario: Cancellation clears active ownership under lock
- **WHEN** cancellation targets execution `e1`
- **THEN** the runtime SHALL lock `e1`'s `cache_key`
- **AND** it SHALL delete `active/<cache_key>` before writing `lifecycle = "cancel-pending"`

#### Scenario: Direct child edges are processed concurrently
- **WHEN** execution `e1` has direct children `e2`, `e3`, and `e4`
- **THEN** the runtime SHALL process those direct child edges through a thread pool
- **AND** it SHALL wait for all submitted child work to finish before marking `e1` `cancelled`

#### Scenario: Runtime does not mutate child lifecycle directly during parent cancel
- **WHEN** cancellation of `e1` reaches child `e2`
- **THEN** the runtime SHALL remove `exec/edges/e2/e1.json`
- **AND** it SHALL invoke the adapter chain responsible for child cancellation
- **BUT** it SHALL NOT write `exec/state/e2.json` as part of `cancel(e1)` unless `cancel(e2)` is entered as its own runtime call

#### Scenario: Synchronous completion waits for child cancel handlers to return
- **WHEN** `cancel(e1)` writes `lifecycle = "cancelled"`
- **THEN** the child worker pool for `e1` SHALL already have finished
- **AND** any nested `cancel(child)` calls delegated from that workflow SHALL already have returned

#### Scenario: Terminal adapter ownership is conventional, not required
- **WHEN** an adapter chain handles one child cancel update
- **THEN** the chain as a whole SHALL own execution and cancellation of that child job
- **AND** the implementation MAY use its terminal adapter as the layer that performs kickoff and cancellation
- **BUT** that terminal-adapter convention SHALL NOT be required by the contract

#### Scenario: Root cancel finalizes after synchronous workflow completion
- **WHEN** `cancel(idx1)` finishes its synchronous workflow
- **THEN** the runtime SHALL persist `exec/state/idx1.json` with `lifecycle = "cancelled"`
- **AND** it SHALL delete `indexes/.cancelled/idx1.json`
