## ADDED Requirements

### Requirement: Dag can freeze and unfreeze a user runtime index

`Dag.freeze(message=None)` SHALL replace its active index token with the frozen reference returned by `Dml.runtime.freeze`. It SHALL pass the runtime a message formatted as `dag: <Dag.name>` and, when its optional message argument is non-empty, append `\n<message>`. `Dag.unfreeze()` SHALL replace its frozen token with the active reference returned by `Dml.runtime.unfreeze`.

#### Scenario: Freeze and thaw preserve the Dag wrapper

- **WHEN** a caller freezes then unfreezes an uncommitted user-index-backed `Dag`
- **THEN** both methods return that same `Dag` instance and update only its token to the runtime-returned reference

### Requirement: Frozen indexes use DAG projections for inspection

For an uncommitted index, including a frozen index, `Dag` SHALL inspect the partial DAG returned by `Dml.runtime.describe(index)["dag"]` through `Dml.dag.describe` for named nodes, keys, values, and argv.

#### Scenario: Read a named node from a frozen index

- **WHEN** a caller reads a named node from a frozen `Dag`
- **THEN** the API resolves the node from the frozen index's described partial DAG without calling a runtime mutation operation

### Requirement: Frozen indexes remain uncommitted and immutable

A frozen `Dag` SHALL not present a result as if it were committed. The API SHALL not implicitly unfreeze a frozen index for mutation.

#### Scenario: Access result on a frozen index

- **WHEN** a caller accesses `Dag.result` on a frozen index
- **THEN** the API raises the existing uncommitted-DAG error
