## MODIFIED Requirements

### Requirement: DAG get resolves by name or exact DAG ref
`dml dag get <value> [--value-type {str,ref}] [--revision REV]` SHALL resolve either a DAG name within a revision's DAG map or an explicit `dag:<id>` selector.

If `--value-type ref` is selected, the command SHALL reject any provided `--revision` flag.

#### Scenario: DAG get resolves name in revision
- **WHEN** a user runs `dml dag get train --value-type str --revision HEAD~1`
- **THEN** the command resolves `train` in the DAG map for `HEAD~1`
- **AND** returns JSON containing `selector`, `revision`, and `dag`

#### Scenario: DAG get loads exact DAG ref
- **WHEN** a user runs `dml dag get dag:abc123 --value-type ref`
- **THEN** the command loads that exact DAG object
- **AND** returns JSON containing `selector` and `dag`

#### Scenario: DAG get rejects revision with explicit DAG ref
- **WHEN** a user runs `dml dag get dag:abc123 --value-type ref --revision HEAD`
- **THEN** the command fails without resolving a revision

#### Scenario: DAG get defaults selector by union order
- **WHEN** a user runs `dml dag get train` without `--value-type`
- **THEN** the command uses the first non-`None` member of the `value` union in annotation order
- **AND** it does not infer the member type from the token text
