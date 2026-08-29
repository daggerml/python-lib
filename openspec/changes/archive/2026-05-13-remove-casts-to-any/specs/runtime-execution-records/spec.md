## ADDED Requirements

### Requirement: Execution record status typing SHALL remain concrete during runtime updates
The runtime SHALL preserve the declared execution-status literal type from adapter results through execution-record creation and execution-record merge operations. The implementation SHALL NOT erase `ExecutionRecord["status"]` through `cast(..., Any)` when persisting or merging runtime execution state.

#### Scenario: First execution record uses the adapter result status directly
- **WHEN** `IndexOps.start_fn` constructs an execution record from a valid adapter result
- **THEN** the record stores the concrete runtime status value without erasing it through `Any`

#### Scenario: Merge preserves the higher-ranked status without type erasure
- **WHEN** execution-record merge logic chooses between current and incoming statuses
- **THEN** it keeps the higher-ranked concrete status value and returns an `ExecutionRecord` whose `status` remains within the declared runtime status set
