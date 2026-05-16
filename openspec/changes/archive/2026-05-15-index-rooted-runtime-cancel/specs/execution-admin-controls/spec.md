## MODIFIED Requirements

### Requirement: Manual cancellation SHALL target index identity
The system SHALL treat cancellation as an index-rooted execution-graph operation keyed by index id. A user cancellation request SHALL lock the target index, atomically move `indexes/<index_id>.json` to `indexes/.cancelled/<index_id>.json`, and plan cancellation from the rooted execution set initialized as `{index_id}`.

The cancellation algorithm SHALL operate as follows:

1. Lock the target index.
2. Move `indexes/<index_id>.json` to `indexes/.cancelled/<index_id>.json` atomically.
3. Release the index lock.
4. Initialize `unseen = {index_id}`.
5. While `unseen` is not empty, remove one candidate id.
6. Acquire that candidate id's lock.
7. While holding the lock, reread `exec/state/<candidate_id>.json`; if it does not exist, release the lock and continue.
8. If `status` is `succeeded`, `failed`, or `cancelled`, release the lock and continue.
9. Add that record's `dependencies` to `unseen`.
10. While still holding the lock, count callers of that candidate id whose state exists and whose status is not `cancel-requested`, `cancelled`, `succeeded`, or `failed`.
11. If that active caller count is `0`, update `exec/state/<candidate_id>.json` with compare-and-swap semantics so that `status = "cancel-requested"` and `cancel_requested_by` identifies the requesting user, then invoke the candidate's adapter update path with `execution_status = "cancel-requested"`.
12. If that cancel update returns terminal `cancelled`, update `exec/state/<candidate_id>.json` so that `status = "cancelled"`.
13. Release the candidate lock.
14. After the bounded sweep completes, delete `indexes/.cancelled/<index_id>.json`.

#### Scenario: Runtime cancel freezes the index before planning
- **WHEN** a user cancels index `idx1`
- **THEN** the system SHALL atomically move `indexes/idx1.json` to `indexes/.cancelled/idx1.json` under lock before cancellation planning begins

#### Scenario: Rooted cancellation starts from the index id itself
- **WHEN** a user cancels index `idx1`
- **THEN** the planner SHALL initialize its rooted work set as `{idx1}`
- **AND** it SHALL read `exec/state/idx1.json` as the synthetic root state record

#### Scenario: Cancellation marks eligible rooted executions cancel-requested
- **WHEN** a rooted execution reachable from the cancelled index has no other active callers
- **THEN** the system SHALL update `exec/state/<execution_id>.json` so that `status = "cancel-requested"`
- **AND** `cancel_requested_by` identifies the requesting user

#### Scenario: Dependencies are added to the rooted work set before cancel dispatch
- **WHEN** the planner examines candidate `e1`
- **THEN** it SHALL add `e1`'s recorded `dependencies` to the rooted work set before deciding whether to invoke adapter cancellation for `e1`

#### Scenario: Cancellation sweep removes the temporary cancelled-index marker
- **WHEN** the runtime completes the bounded cancellation sweep for index `idx1`
- **THEN** it SHALL delete `indexes/.cancelled/idx1.json`

### Requirement: Cancellation propagation SHALL stop when a callee still has a live caller
The local planner SHALL propagate cancellation only across non-terminal rooted dependency records. It SHALL stop recursing when it reaches `succeeded`, `failed`, or `cancelled`. Among non-terminal records in the dependency closure, it SHALL request cancellation only when a candidate record has no remaining active callers. For this algorithm, an active caller is a caller record whose state exists and whose `status` is not `cancel-requested`, `cancelled`, `succeeded`, or `failed`.

#### Scenario: Shared dependency is preserved while another caller remains live
- **WHEN** execution `e2` depends on `e3` and a different active execution `e4` also depends on `e3`
- **THEN** cancelling the rooted index for `e2` SHALL NOT require `e3` to be cancelled while `e4` remains an active caller

#### Scenario: Cancel-requested caller is not active
- **WHEN** execution `e3` is called only by executions whose status is `cancel-requested`, `cancelled`, `succeeded`, or `failed`
- **THEN** the planner SHALL treat `e3` as having no active callers for cancellation eligibility

#### Scenario: Terminal dependency is not cancelled
- **WHEN** execution `e2` depends on execution `e3` and `e3` is already terminal
- **THEN** cancelling the rooted index for `e2` SHALL NOT request cancellation for `e3`
