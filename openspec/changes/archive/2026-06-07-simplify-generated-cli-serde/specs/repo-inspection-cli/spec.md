## MODIFIED Requirements

### Requirement: DAG get resolves by name or exact DAG ref
`dml dag get <value> [--revision REV]` SHALL resolve either a DAG name within a revision's DAG map or an explicit `dag:<id>` selector.

The command SHALL not expose `--value-type` or any other explicit union transport selector. Instead it SHALL use the generated CLI's ordered parser-family rules for the `value` annotation.

#### Scenario: DAG get resolves name in revision
- **WHEN** a user runs `dml dag get train --revision HEAD~1`
- **THEN** the command resolves `train` in the DAG map for `HEAD~1`
- **AND** returns JSON containing `selector`, `revision`, and `dag`

#### Scenario: DAG get loads exact DAG ref when the parser reaches ref construction
- **WHEN** a user runs `dml dag get dag:abc123`
- **THEN** the generated CLI first applies the higher-priority parser families allowed by the annotation
- **AND** if those families do not yield an accepted value, it falls back to exact `Ref` construction
- **AND** returns JSON containing `selector` and `dag`

#### Scenario: DAG get rejects revision with explicit DAG ref
- **WHEN** a user runs `dml dag get dag:abc123 --revision HEAD`
- **THEN** the command fails without resolving a revision when the resolved CLI value is an explicit DAG ref

#### Scenario: DAG get uses ordered parsing instead of explicit selector flags
- **WHEN** a user runs `dml dag get train`
- **THEN** the command uses the generated CLI parser order for the `value` annotation
- **AND** it does not expose or require `--value-type`
