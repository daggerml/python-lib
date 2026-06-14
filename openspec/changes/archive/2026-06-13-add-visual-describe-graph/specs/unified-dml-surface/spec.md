## MODIFIED Requirements

### Requirement: Shared `Dml` runtime namespace SHALL normalize roots for execution graph inspection
The shared `Dml` runtime namespace SHALL expose `describe_graph(*roots: Ref | str, visual: bool = False)` for execution-lineage inspection. If the caller provides no roots, the runtime namespace SHALL use all currently open local runtime indexes as roots. Before delegating to execution-state graph extraction, the runtime namespace SHALL normalize the selected roots to execution-id strings. When `visual` is `False`, the method SHALL return the extracted `ExecutionGraph`. When `visual` is `True`, the method SHALL render a human-friendly graph view and return `None`.

#### Scenario: Explicit roots are normalized and delegated
- **WHEN** a caller invokes `dml.runtime.describe_graph(idx1, "exec-2")`
- **THEN** the runtime namespace SHALL normalize those roots to execution-id strings
- **AND** it SHALL delegate the graph extraction using only those normalized root ids

#### Scenario: Empty input defaults to open local indexes
- **WHEN** a caller invokes `dml.runtime.describe_graph()` with no explicit roots
- **THEN** the runtime namespace SHALL read the currently open local runtime indexes
- **AND** it SHALL use those index ids as the root execution ids for graph extraction

#### Scenario: Visual mode renders instead of returning the raw graph
- **WHEN** a caller invokes `dml.runtime.describe_graph(idx1, visual=True)`
- **THEN** the runtime namespace SHALL fetch the same execution graph data it would use for raw inspection
- **AND** it SHALL render a human-friendly execution graph view
- **AND** it SHALL return `None`
