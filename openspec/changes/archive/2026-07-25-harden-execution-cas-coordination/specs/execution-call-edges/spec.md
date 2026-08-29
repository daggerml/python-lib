## ADDED Requirements

### Requirement: Failed child registration SHALL roll back unrealized caller edges
When a launch attempt writes a caller/callee edge but fails to durably register the child in the caller's execution record, the runtime SHALL remove that attempt's edge before surfacing the registration failure. If the attempt created a fresh active execution, it SHALL also clean up only the active and reservation artifacts owned by that attempt.

#### Scenario: Fresh child registration failure cleans up attempt artifacts
- **WHEN** a launch attempt creates fresh child `e1` and its registration under caller `e0` exhausts retries
- **THEN** the runtime SHALL remove the `e1 <- e0` caller edge
- **AND** it SHALL remove the fresh attempt's active and reservation artifacts

#### Scenario: Reused child registration failure preserves shared artifacts
- **WHEN** a launch attempt reuses active child `e1` and its registration under caller `e0` fails
- **THEN** the runtime SHALL remove the `e1 <- e0` caller edge
- **AND** it SHALL not remove `e1`'s shared active or reservation artifacts
