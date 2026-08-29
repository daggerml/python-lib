## MODIFIED Requirements

### Requirement: Manual cancellation SHALL target index identity
The system SHALL treat cancellation as an index-rooted execution-graph operation keyed by index id. `Dml.runtime.cancel` SHALL lock the target index, atomically move `indexes/<index_id>.json` to `indexes/.cancelled/<index_id>.json`, mark the synthetic index execution record `cancel-requested`, traverse the full rooted execution graph, and run a retryable cancellation loop over the rooted candidate executions.

The cancellation algorithm SHALL operate as follows:

1. Lock the target index.
2. Move `indexes/<index_id>.json` to `indexes/.cancelled/<index_id>.json` atomically.
3. Release the index lock.
4. Ensure `exec/state/<index_id>.json` exists as the synthetic root state record.
5. Update `exec/state/<index_id>.json` with compare-and-swap semantics so that `status = "cancel-requested"` and `cancel_requested_by` identifies the requesting user before any descendant cancellation work begins.
6. Traverse the full execution graph rooted at the synthetic root record's `dependencies` and collect `graph := {(caller, callee), ...}`.
7. Define `candidate_set := {callee | (caller, callee) in graph}` and `own_executions := candidate_set.copy()`.
8. While `candidate_set` is not empty, `Dml.runtime.cancel` SHALL run a parallel worker across the current `candidate_set` and log loop diagnostics.
9. For each candidate execution id, attempt to acquire the candidate's `cache_key` lock; if lock acquisition fails, return `None` for that candidate.
10. While holding the lock, reread `exec/state/<candidate_id>.json`; if it does not exist, release the lock and return `-1`.
11. While holding the lock, read `active_callers(c)` for the candidate from the global reverse-edge records in S3 and determine current `status`.
12. If `len(active_callers(c) - own_executions) > 0` or the candidate is not in an active status, release the lock and return `-1`.
13. Otherwise, update `exec/state/<candidate_id>.json` with compare-and-swap semantics so that `status = "cancel-requested"` and `cancel_requested_by` identifies the requesting user before invoking that candidate's adapter update path with `execution_status = "cancel-requested"`.
14. If the candidate's full adapter chain reaches terminal `cancelled`, update `exec/state/<candidate_id>.json` so that `status = "cancelled"`, release the lock, and return `+1`.
15. Otherwise, release the lock and return `None`.
16. After one loop iteration completes, remove every `+1` candidate from `candidate_set` and remove every `-1` candidate from both `candidate_set` and `own_executions`.
17. Repeat until `candidate_set` is empty.
18. After every execution remaining in `own_executions` has status `cancelled`, update `exec/state/<index_id>.json` so that `status = "cancelled"`.
19. After the rooted graph has been cancelled successfully for the index-owned executions, delete `indexes/.cancelled/<index_id>.json`.
20. `Dml.runtime.cancel` SHALL return a cancellation statistics object.

`Dml.runtime.cancel` MAY continue looping indefinitely when a candidate execution can only be fully cancelled by adapters or runtimes that are unreachable from the index runtime process. In that case, the runtime SHALL continue persisting and observing `cancel-requested` state but is not required to guarantee autonomous completion.

The cancellation statistics object SHALL have the following schema:

- `index_id: str`
- `iterations: int`
- `graph_edges: int`
- `candidate_count: int`
- `own_execution_count: int`
- `cancelled_count: int`
- `dropped_count: int`
- `lock_retry_count: int`

#### Scenario: Runtime cancel freezes the index before planning
- **WHEN** a user cancels index `idx1`
- **THEN** the system SHALL atomically move `indexes/idx1.json` to `indexes/.cancelled/idx1.json` under lock before cancellation planning begins

#### Scenario: Rooted cancellation starts from the index root dependencies
- **WHEN** a user cancels index `idx1`
- **THEN** the runtime SHALL update `exec/state/idx1.json` so that `status = "cancel-requested"`
- **AND** it SHALL initialize rooted graph traversal from `exec/state/idx1.json` dependencies rather than from `{idx1}` itself

#### Scenario: Root cancellation is recorded before descendant work
- **WHEN** a user cancels index `idx1`
- **THEN** the runtime SHALL persist `exec/state/idx1.json` with `status = "cancel-requested"` before counting callers for descendants or invoking any adapter cancellation updates

#### Scenario: Cancellation discovers the full rooted graph before processing
- **WHEN** a user cancels index `idx1`
- **THEN** the runtime SHALL traverse the full execution graph reachable from `exec/state/idx1.json` dependencies
- **AND** it SHALL collect caller-callee edges for the full rooted graph before processing cancellation decisions for candidate executions

#### Scenario: Candidate and ownership sets are initialized from rooted traversal
- **WHEN** rooted graph traversal for index `idx1` produces caller-callee graph `G`
- **THEN** the runtime SHALL derive `candidate_set` from the callee nodes in `G`
- **AND** it SHALL initialize `own_executions` as a copy of `candidate_set`

#### Scenario: Caller ownership uses the global reverse-edge set
- **WHEN** index `A` and unrelated index `B` both call execution `X`
- **THEN** `callers(X)` SHALL include both `A` and `B`
- **AND** `cancel(A)` SHALL NOT cancel `X`

#### Scenario: Recursive ownership remains cancellable
- **WHEN** index `A` calls `X` and `Y`
- **AND** `Y` calls `X`
- **AND** the global caller set is `callers(X) = {A, Y}`
- **THEN** `cancel(A)` MAY cancel `X`

#### Scenario: Candidate lock contention yields retry
- **WHEN** the loop examines candidate execution `e1`
- **AND** `e1`'s cache-key lock cannot be acquired
- **THEN** the worker SHALL return `None`
- **AND** the loop SHALL leave `e1` in `candidate_set` for retry

#### Scenario: Cancellation loop reports diagnostics
- **WHEN** `Dml.runtime.cancel` runs one or more cancellation loop iterations
- **THEN** it SHALL emit diagnostics describing loop progress

#### Scenario: Cancellation returns loop statistics
- **WHEN** `Dml.runtime.cancel` completes for index `idx1`
- **THEN** it SHALL return cancellation statistics
- **AND** those statistics SHALL include the number of loop iterations

#### Scenario: Cancellation statistics report rooted graph size
- **WHEN** rooted graph traversal for `idx1` collects 7 caller-callee edges and 4 candidate executions
- **THEN** the returned statistics SHALL include `graph_edges = 7`
- **AND** they SHALL include `candidate_count = 4`

#### Scenario: Cancellation statistics report loop outcomes
- **WHEN** one cancellation run for `idx1` cancels 2 executions, drops 1 execution from ownership, and retries 3 lock-contention events
- **THEN** the returned statistics SHALL include `cancelled_count = 2`
- **AND** they SHALL include `dropped_count = 1`
- **AND** they SHALL include `lock_retry_count = 3`

#### Scenario: Cancellation statistics identify the target index
- **WHEN** `Dml.runtime.cancel` completes for index `idx1`
- **THEN** the returned statistics SHALL include `index_id = "idx1"`

#### Scenario: Candidate cancellation runs only without active callers
- **WHEN** the planner examines candidate execution `e1`
- **AND** `e1` still has at least one active caller outside `own_executions`
- **THEN** the runtime SHALL NOT mark `exec/state/e1.json` as `cancel-requested`
- **AND** it SHALL NOT invoke adapter cancellation for `e1`
- **AND** it SHALL NOT mark `exec/state/e1.json` as `cancelled`
- **AND** it SHALL remove `e1` from both `candidate_set` and `own_executions` for the current cancellation run

#### Scenario: Candidate cancel request is recorded before cancellation work
- **WHEN** the planner examines candidate execution `e1`
- **AND** `e1` has no active callers outside `own_executions`
- **THEN** the runtime SHALL persist `exec/state/e1.json` with `status = "cancel-requested"` before invoking adapter cancellation for `e1`

#### Scenario: Active callers are rechecked under lock
- **WHEN** execution `e1` is in the current `candidate_set`
- **AND** the runtime has acquired `e1`'s cache-key lock for the current loop iteration
- **THEN** it SHALL recompute `e1`'s active-caller set before marking `cancel-requested` or invoking adapter cancellation

#### Scenario: Terminal cancelled waits for the full adapter chain
- **WHEN** execution `e1` has no active callers outside `own_executions`
- **AND** one adapter layer reports cancellation progress before outer adapter cleanup has finished
- **THEN** the runtime SHALL keep `exec/state/e1.json` at `status = "cancel-requested"`
- **AND** the index-cancellation runtime SHALL NOT persist `status = "cancelled"` until the full adapter chain has completed cancellation handling

#### Scenario: Unreachable remote-only adapter chain can stall cancellation
- **WHEN** execution `e1` delegates cancellation work to descendant execution `e2`
- **AND** completing cancellation for `e2` requires bespoke adapters or a runtime unreachable from the index runtime process
- **THEN** `Dml.runtime.cancel` MAY continue retrying without converging to terminal `cancelled`
- **AND** it SHALL keep the relevant execution records at `status = "cancel-requested"` until another runtime handles cancellation or the user interrupts the loop

#### Scenario: Cancelled execution still does not prune graph traversal
- **WHEN** execution `e1` is already `cancelled`
- **AND** `e1` has recorded dependencies
- **THEN** the runtime SHALL NOT invoke adapter cancellation for `e1`
- **AND** it SHALL still include `e1`'s descendants in rooted graph traversal for index cancellation

#### Scenario: Successful cancellation removes only the candidate from the retry set
- **WHEN** the loop worker for execution `e1` returns `+1`
- **THEN** the runtime SHALL remove `e1` from `candidate_set`
- **AND** it SHALL keep `e1` in `own_executions`

#### Scenario: Failed ownership removes candidate from both sets
- **WHEN** the loop worker for execution `e1` returns `-1`
- **THEN** the runtime SHALL remove `e1` from `candidate_set`
- **AND** it SHALL remove `e1` from `own_executions`

#### Scenario: Cancellation sweep marks the synthetic root cancelled after graph completion
- **WHEN** the runtime completes the retry loop for index `idx1`
- **AND** every execution remaining in `own_executions` has status `cancelled`
- **THEN** it SHALL update `exec/state/idx1.json` so that `status = "cancelled"`
- **AND** it SHALL delete `indexes/.cancelled/<index_id>.json`
